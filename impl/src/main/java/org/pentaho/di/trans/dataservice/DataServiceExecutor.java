/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.dataservice;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.MultimapBuilder;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaAndData;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransAdapter;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.clients.TransMutators;
import org.pentaho.di.trans.dataservice.execution.CopyParameters;
import org.pentaho.di.trans.dataservice.execution.DefaultTransWiring;
import org.pentaho.di.trans.dataservice.execution.PrepareExecution;
import org.pentaho.di.trans.dataservice.execution.TransStarter;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
import org.pentaho.di.trans.dataservice.streaming.execution.StreamingGeneratedTransExecution;
import org.pentaho.di.trans.dataservice.streaming.execution.StreamingServiceTransExecutor;
import org.pentaho.di.trans.dataservice.utils.DataServiceConstants;
import org.pentaho.di.trans.dataservice.utils.KettleUtils;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.metastore.api.IMetaStore;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public class DataServiceExecutor {
  private static final Class<?> PKG = DataServiceExecutor.class;

  private final Trans serviceTrans;
  private final Trans genTrans;

  private final DataServiceMeta service;
  private final SQL sql;

  private final Map<String, String> parameters;
  private final SqlTransGenerator sqlTransGenerator;
  private final ListMultimap<ExecutionPoint, Runnable> listenerMap;
  private final DataServiceContext context;

  private IDataServiceClientService.StreamingMode windowMode;
  private long windowSize;
  private long windowEvery;
  private long windowLimit;

  private DataServiceExecutor( Builder builder ) {
    sql = builder.sql;
    service = builder.service;
    parameters = Maps.newHashMap( builder.parameters );
    parameters.putAll( getWhereConditionParameters() );
    serviceTrans = builder.serviceTrans;
    sqlTransGenerator = builder.sqlTransGenerator;
    genTrans = builder.genTrans;
    context = builder.context;
    windowMode = builder.windowMode;
    windowSize = builder.windowSize;
    windowEvery = builder.windowEvery;
    windowLimit = builder.windowLimit;

    listenerMap = MultimapBuilder.enumKeys( ExecutionPoint.class ).linkedListValues().build();
  }

  public static class Builder {
    private final SQL sql;
    private final DataServiceMeta service;
    private final DataServiceContext context;
    private Trans serviceTrans;
    private Trans genTrans;
    private int rowLimit = 0;
    private long timeLimit = 0;
    private IDataServiceClientService.StreamingMode windowMode;
    private long windowSize = 0;
    private long windowEvery = 0;
    private long windowLimit = 0;
    private Map<String, String> parameters = Collections.emptyMap();
    private LogLevel logLevel;
    private SqlTransGenerator sqlTransGenerator;

    private boolean normalizeConditions = true;
    private boolean prepareExecution = true;
    private boolean enableMetrics = false;
    private IMetaStore metastore;
    private BiConsumer<String, TransMeta> transMutator =
      ( stepName, transMeta ) -> TransMutators.disableAllUnrelatedHops( stepName, transMeta, true );

    private KettleUtils kettleUtils = KettleUtils.getInstance();

    public Builder( SQL sql, DataServiceMeta service, DataServiceContext context ) {
      this.sql = Preconditions.checkNotNull( sql, "SQL must not be null." );
      this.service = Preconditions.checkNotNull( service, "Service must not be null." );
      this.context = context;
    }

    public Builder parameters( Map<String, String> parameters ) {
      this.parameters = parameters;
      return this;
    }

    public Builder rowLimit( int rowLimit ) {
      this.rowLimit = rowLimit;
      return this;
    }

    public Builder timeLimit( long timeLimit ) {
      this.timeLimit = timeLimit;
      return this;
    }

    public Builder windowMode( IDataServiceClientService.StreamingMode windowMode ) {
      this.windowMode = windowMode;
      return this;
    }

    public Builder windowSize( long windowSize ) {
      this.windowSize = windowSize;
      return this;
    }

    public Builder windowEvery( long windowEvery ) {
      this.windowEvery = windowEvery;
      return this;
    }

    public Builder windowLimit( long windowLimit ) {
      this.windowLimit = windowLimit;
      return this;
    }

    public Builder logLevel( LogLevel logLevel ) {
      this.logLevel = logLevel;
      return this;
    }

    public Builder metastore( final IMetaStore metastore ) {
      this.metastore = metastore;
      return this;
    }

    public Builder serviceTrans( Trans serviceTrans ) {
      this.serviceTrans = serviceTrans;
      return this;
    }

    Builder serviceTransMutator( BiConsumer<String, TransMeta> transMutator ) {
      this.transMutator = transMutator;
      return this;
    }

    public Builder serviceTrans( TransMeta serviceTransMeta ) {
      // Copy TransMeta, we don't want to persist any changes to the meta during execution
      serviceTransMeta = (TransMeta) serviceTransMeta.realClone( false );
      serviceTransMeta.clearNameChangedListeners();
      serviceTransMeta.setName( calculateTransname( sql, true ) );
      serviceTransMeta.activateParameters();
      transMutator.accept( service.getStepname(), serviceTransMeta );
      return serviceTrans( new Trans( serviceTransMeta ) );
    }

    public Builder sqlTransGenerator( SqlTransGenerator sqlTransGenerator ) {
      this.sqlTransGenerator = sqlTransGenerator;
      return this;
    }

    public Builder genTrans( Trans trans ) {
      this.genTrans = trans;
      return this;
    }

    public Builder enableMetrics( boolean enable ) {
      enableMetrics = enable;
      return this;
    }

    public Builder normalizeConditions( boolean enable ) {
      normalizeConditions = enable;
      return this;
    }

    public Builder prepareExecution( boolean enable ) {
      prepareExecution = enable;
      return this;
    }

    public DataServiceExecutor build() throws KettleException {
      RowMetaInterface serviceFields;
      int serviceRowLimit = getServiceRowLimit( service );

      if ( sql.getServiceName() != null && !sql.getServiceName().equals( service.getName() ) ) {
        throw new KettleException(
          BaseMessages.getString( PKG, "DataServiceExecutor.Error.TableNameAndDataServiceNameDifferent",
            sql.getServiceName(), service.getName() ) );
      }

      if ( serviceTrans == null && service.getServiceTrans() != null ) {
        serviceTrans( service.getServiceTrans() );
      }

      // Check if theres already a serviceTransformation in the context
      if ( service.isStreaming() ) {
        StreamingServiceTransExecutor serviceTransExecutor = context.getServiceTransExecutor( service.getName() );

        // Gets service execution from context
        if ( serviceTransExecutor == null ) {
          if ( serviceTrans != null ) {
            int windowMaxRowLimit = (int) getStreamingLimit( rowLimit, service.getRowLimit(), getKettleRowLimit(),
              DataServiceConstants.ROW_LIMIT_DEFAULT );
            long windowMaxTimeLimit = getStreamingLimit( timeLimit, service.getTimeLimit(), getKettleTimeLimit(),
              DataServiceConstants.TIME_LIMIT_DEFAULT );

            serviceTransExecutor =
              new StreamingServiceTransExecutor( service.getName(), serviceTrans, service.getStepname(),
                windowMaxRowLimit, windowMaxTimeLimit );
            context.addServiceTransExecutor( serviceTransExecutor );
          }
        } else {
          serviceTrans( serviceTransExecutor.getServiceTrans() );
        }
      }

      // Sets the service transformation fields
      if ( serviceTrans != null ) {
        serviceFields = serviceTrans.getTransMeta().getStepFields( service.getStepname() );
      } else {
        throw new KettleException(
          BaseMessages.getString( PKG, "DataServiceExecutor.Error.NoServiceTransformation",
            sql.getServiceName(), service.getName() ) );
      }

      ValueMetaResolver resolver = new ValueMetaResolver( serviceFields );

      sql.parse( resolver.getRowMeta() );

      if ( normalizeConditions ) {
        if ( sql.getWhereCondition() != null && sql.getWhereCondition().getCondition() != null ) {
          convertCondition( sql.getWhereCondition().getCondition(), resolver );
        }
        if ( sql.getHavingCondition() != null && sql.getHavingCondition().getCondition() != null ) {
          convertCondition( sql.getHavingCondition().getCondition(), resolver );
        }
      }

      if ( sqlTransGenerator == null ) {
        sqlTransGenerator = new SqlTransGenerator( sql, service.isStreaming() ? 0 : rowLimit,
          service.isStreaming() ? 0
            : ( serviceRowLimit > 0 ? serviceRowLimit
              : ( !service.isUserDefined() ? DataServiceConstants.ROW_LIMIT_DEFAULT : 0 ) ) );
      }
      if ( genTrans == null ) {
        genTrans = new Trans( sqlTransGenerator.generateTransMeta() );
      }

      serviceTrans.setContainerObjectId( UUID.randomUUID().toString() );
      serviceTrans.setMetaStore( metastore );
      serviceTrans.setGatheringMetrics( enableMetrics );

      genTrans.setContainerObjectId( UUID.randomUUID().toString() );
      genTrans.setMetaStore( metastore );
      genTrans.setGatheringMetrics( enableMetrics );

      DataServiceExecutor dataServiceExecutor = new DataServiceExecutor( this );

      context.addExecutor( dataServiceExecutor );

      if ( logLevel != null ) {
        dataServiceExecutor.setLogLevel( logLevel );
      }

      if ( prepareExecution ) {
        dataServiceExecutor.prepareExecution();
      }

      return dataServiceExecutor;
    }

    /**
     * Gets the streaming dataservice max limit for a window.
     *
     * @param userLimit - The user limit.
     * @param serviceLimit - The metadata limit.
     * @param kettleLimit - The kettle limit.
     * @param defaultLimit - The default limit.
     * @return The streaming max limit.
     * @throws KettleException
     */
    private long getStreamingLimit( long userLimit, long serviceLimit, long kettleLimit, long defaultLimit ) throws KettleException {
      if ( serviceLimit > 0 && kettleLimit > 0 && userLimit > 0 ) {
        return Math.min( Math.min( serviceLimit, kettleLimit ), userLimit );
      } else if ( serviceLimit > 0 && kettleLimit > 0 ) {
        return Math.min( serviceLimit, kettleLimit );
      } else if ( serviceLimit > 0 && userLimit > 0 ) {
        return Math.min( serviceLimit, userLimit );
      } else if ( kettleLimit > 0 && userLimit > 0 ) {
        return Math.min( kettleLimit, userLimit );
      } else if ( kettleLimit > 0 ) {
        return kettleLimit;
      } else if ( serviceLimit > 0 ) {
        return serviceLimit;
      } else if ( userLimit > 0 ) {
        return userLimit;
      }
      return defaultLimit;
    }

    private int getServiceRowLimit( DataServiceMeta service ) throws KettleException {
      if ( service.getRowLimit() > 0 ) {
        return service.getRowLimit();
      } else if ( !service.isUserDefined() || service.isStreaming() ) {
        return getKettleRowLimit();
      }
      return 0;
    }

    private int getKettleRowLimit() throws KettleException {
      String limit = kettleUtils.getKettleProperty( DataServiceConstants.ROW_LIMIT_PROPERTY );
      int result = 0;

      if ( limit == null || limit.isEmpty() ) {
        limit = kettleUtils.getKettleProperty( DataServiceConstants.LEGACY_LIMIT_PROPERTY );
      }

      if ( !Utils.isEmpty( limit ) ) {
        try {
          result = Integer.parseInt( limit );
        } catch ( NumberFormatException e ) {
          if ( context != null && context.getLogChannel() != null ) {
            context.getLogChannel().logError( String.format( "%s: %s ", DataServiceConstants.ROW_LIMIT_PROPERTY, e ) );
          }
        }
      }

      return result;
    }

    private long getKettleTimeLimit() throws KettleException {
      String limit = kettleUtils.getKettleProperty( DataServiceConstants.TIME_LIMIT_PROPERTY );
      long result = 0;
      if ( !Utils.isEmpty( limit ) ) {
        try {
          result = Long.parseLong( limit );
        } catch ( NumberFormatException e ) {
          if ( context != null && context.getLogChannel() != null ) {
            context.getLogChannel().logError( String.format( "%s: %s ", DataServiceConstants.TIME_LIMIT_PROPERTY, e ) );
          }
        }
      }
      return result;
    }
  }

  private void setLogLevel( LogLevel logLevel ) {
    if ( serviceTrans != null ) {
      serviceTrans.setLogLevel( logLevel );
      getServiceTransMeta().setLogLevel( logLevel );
    }
    if ( genTrans != null ) {
      genTrans.setLogLevel( logLevel );
      getGenTransMeta().setLogLevel( logLevel );
    }
  }

  private static void convertCondition( Condition condition, ValueMetaResolver resolver ) {
    if ( condition.isAtomic() ) {
      if ( condition.getFunction() == Condition.FUNC_IN_LIST ) {
        convertListCondition( condition, resolver );
      } else {
        convertAtomicCondition( condition, resolver );
      }
    } else {
      for ( Condition child : condition.getChildren() ) {
        convertCondition( child, resolver );
      }
    }
  }

  private static void convertAtomicCondition( Condition condition, ValueMetaResolver resolver ) {
    // No need to convert conditions with no right side argument
    if ( condition.getRightExact() == null ) {
      return;
    }
    String fieldName = condition.getLeftValuename();
    ValueMetaAndData rhs = condition.getRightExact();
    try {
      // Determine meta and resolve value
      ValueMetaInterface resolvedValueMeta = resolver.getValueMeta( fieldName );
      if ( ValueMetaInterface.TYPE_INTEGER == resolvedValueMeta.getType() && ValueMetaInterface.TYPE_NUMBER == rhs.getValueMeta().getType() ) {
        // BACKLOG-18738
        // Do not round Double parameter value
        return;
      }
      Object resolvedValue = resolver.getTypedValue( fieldName, rhs.getValueMeta().getType(), rhs.getValueData() );

      // We have normally stored object here, adjust value meta accordingly
      if ( resolvedValueMeta.getStorageType() != ValueMetaInterface.STORAGE_TYPE_NORMAL ) {
        resolvedValueMeta = resolvedValueMeta.clone();
        resolvedValueMeta.setStorageType( ValueMetaInterface.STORAGE_TYPE_NORMAL );
      }

      // Set new condition meta and value
      condition.setRightExact( new ValueMetaAndData( resolvedValueMeta, resolvedValue ) );
    } catch ( KettleException e ) {
      // Skip conversion of this condition?
    }
  }

  private static void convertListCondition( Condition condition, ValueMetaResolver resolver ) {
    String fieldName = condition.getLeftValuename();
    try {
      // Determine meta and resolve values
      ValueMetaInterface resolvedValueMeta = resolver.getValueMeta( fieldName );
      Object[] typedValues = resolver.inListToTypedObjectArray( fieldName, condition.getRightExactString() );

      // Encode list values
      String[] typedValueStrings = new String[typedValues.length];
      for ( int i = 0; i < typedValues.length; i++ ) {
        typedValueStrings[i] = resolvedValueMeta.getCompatibleString( typedValues[i] );
      }

      // Set new condition in-list (leave meta as string)
      condition.getRightExact().setValueData( StringUtils.join( typedValueStrings, ';' ) );
    } catch ( KettleException e ) {
      // Skip conversion of this condition?
    }
  }

  private void extractConditionParameters( Condition condition, Map<String, String> parameters ) {
    if ( condition.isAtomic() ) {
      if ( condition.getFunction() == Condition.FUNC_TRUE ) {
        parameters.put( condition.getLeftValuename(), condition.getRightExactString() );
        stripFieldNamesFromTrueFunction( condition );
      }
    } else {
      for ( Condition sub : condition.getChildren() ) {
        extractConditionParameters( sub, parameters );
      }
    }
  }

  /**
   * Strips the "fake" values from a Condition used
   * to pass parameter key/value info in the WHERE clause.
   * E.g. if
   * WHERE PARAMETER('foo') = 'bar'
   * is in the where clause a FUNC_TRUE condition will be
   * created with leftValueName = 'foo' and rightValueName = 'bar'.
   * These need to be stripped out to avoid failing checks
   * for existent field names.
   */
  private void stripFieldNamesFromTrueFunction( Condition condition ) {
    assert condition.getFunction() == Condition.FUNC_TRUE;
    condition.setLeftValuename( null );
    condition.setRightValuename( null );
  }

  protected void prepareExecution() throws KettleException {
    // Setup executor with streaming execution plan
    ImmutableMultimap.Builder<ExecutionPoint, Runnable> builder = ImmutableMultimap.builder();

    builder.putAll( ExecutionPoint.PREPARE,
      new CopyParameters( parameters, serviceTrans ),
      new PrepareExecution( serviceTrans )
    );

    if ( !service.isStreaming() ) {
      builder.put( ExecutionPoint.PREPARE, new PrepareExecution( genTrans ) );

      builder.putAll( ExecutionPoint.READY,
        new DefaultTransWiring( this )
      );

      builder.putAll( ExecutionPoint.START,
        new TransStarter( genTrans ),
        new TransStarter( serviceTrans )
      );
    }
    listenerMap.putAll( builder.build() );
  }

  private Map<String, String> getWhereConditionParameters() {
    // Parameters: see which ones are defined in the SQL
    //
    Map<String, String> conditionParameters = new HashMap<>();
    if ( sql.getWhereCondition() != null ) {
      extractConditionParameters( sql.getWhereCondition().getCondition(), conditionParameters );
    }
    return conditionParameters;
  }

  public static void writeMetadata( DataOutputStream dos, String... metadatas ) throws IOException {
    for ( String metadata : metadatas ) {
      dos.writeUTF( metadata );
    }
  }

  public DataServiceExecutor executeQuery( final DataOutputStream dos ) throws IOException {
    writeMetadata( dos, getServiceName(), calculateTransname( getSql(), true ),
        getServiceTrans().getContainerObjectId(), calculateTransname( getSql(), false ),
        getGenTrans().getContainerObjectId() );

    final AtomicBoolean rowMetaWritten = new AtomicBoolean( false );

    // When done, check if no row metadata was written.  The client is still going to expect it...
    // Since we know it, we'll pass it.
    //
    getGenTrans().addTransListener( new TransAdapter() {
      @Override
      public void transFinished( Trans trans ) throws KettleException {
        if ( rowMetaWritten.compareAndSet( false, true ) ) {
          RowMetaInterface stepFields = trans.getTransMeta().getStepFields( getResultStepName() );
          stepFields.writeMeta( dos );
        }
      }
    } );

    // Now execute the query transformation(s) and pass the data to the output stream...
    return executeQuery( new RowAdapter() {
      @Override
      public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {

        // On the first row, write the metadata...
        //
        try {
          if ( rowMetaWritten.compareAndSet( false, true ) ) {
            rowMeta.writeMeta( dos );
          }
          rowMeta.writeData( dos, row );
        } catch ( Exception e ) {
          if ( !getServiceTrans().isStopped() ) {
            throw new KettleStepException( e );
          }
        }
      }
    } );
  }

  public DataServiceExecutor executeQuery( final RowListener resultRowListener ) {
    if ( service.isStreaming() ) {
      return executeStreamingQuery( resultRowListener );
    } else {
      return executeDefaultQuery( resultRowListener );
    }
  }

  public DataServiceExecutor executeStreamingQuery( final RowListener resultRowListener ) {
    StreamingGeneratedTransExecution streamWiring = new StreamingGeneratedTransExecution( context.getServiceTransExecutor( service.getName() ),
      genTrans, resultRowListener, sqlTransGenerator.getInjectorStepName(), sqlTransGenerator.getResultStepName(),
      sqlTransGenerator.getSql().getSqlString(), windowMode, windowSize, windowEvery, windowLimit );

    listenerMap.get( ExecutionPoint.READY ).add( streamWiring );

    return executeQuery();
  }

  public DataServiceExecutor executeDefaultQuery( final RowListener resultRowListener ) {
    listenerMap.get( ExecutionPoint.READY ).add( new Runnable() {
      @Override public void run() {
        // Give back the eventual result rows...
        //
        StepInterface resultStep = genTrans.findRunThread( getResultStepName() );

        if ( resultStep != null ) {
          resultStep.addRowListener( resultRowListener );
        }
      }
    } );
    return executeQuery();
  }

  public DataServiceExecutor executeQuery() {
    if ( !service.isStreaming() ) {
      // Apply Push Down Optimizations
      for ( PushDownOptimizationMeta optimizationMeta : service.getPushDownOptimizationMeta() ) {
        if ( optimizationMeta.isEnabled() ) {
          optimizationMeta.activate( this );
        }
      }
    }

    // Run execution plan
    executeListeners( ExecutionPoint.values() );

    return this;
  }

  public void executeListeners( ExecutionPoint... stages ) {
    for ( ExecutionPoint stage : stages ) {
      // Copy stage tasks to a new list to prevent accidental concurrent modification
      ImmutableList<Runnable> tasks = ImmutableList.copyOf( listenerMap.get( stage ) );
      for ( Runnable task : tasks ) {
        task.run();
      }
      if ( !listenerMap.get( stage ).equals( tasks ) ) {
        getGenTrans().getLogChannel().logError(
          "Listeners were modified while executing {0}. Started with {1} and ended with {2}",
          stage, tasks, listenerMap.get( stage )
        );
      }
    }
  }

  public RowProducer addRowProducer() throws KettleException {
    return genTrans.addRowProducer( sqlTransGenerator.getInjectorStepName(), 0 );
  }

  public void waitUntilFinished() {
    if ( !service.isStreaming() ) {
      serviceTrans.waitUntilFinished();
    } else {
      try {
        // we need to actively wait for the genTrans to finish, because otherwise it will return on the
        // waitUntilFinished since it didn't had time to start
        while ( !genTrans.isFinishedOrStopped() ) {
          Thread.sleep( 50 );
        }
      } catch ( InterruptedException e ) {
        throw new RuntimeException( e );
      }
    }
    genTrans.waitUntilFinished();
  }

  /**
   * @return the serviceTransMeta
   */
  public TransMeta getServiceTransMeta() {
    return serviceTrans.getTransMeta();
  }

  /**
   * @return the genTransMeta
   */
  public TransMeta getGenTransMeta() {
    return genTrans.getTransMeta();
  }

  public DataServiceMeta getService() {
    return service;
  }

  /**
   * @return the serviceTrans
   */
  public Trans getServiceTrans() {
    return serviceTrans;
  }

  /**
   * @return the genTrans
   */
  public Trans getGenTrans() {
    return genTrans;
  }

  /**
   * @return the serviceName
   */
  public String getServiceName() {
    return sql.getServiceName();
  }

  public Map<String, String> getParameters() {
    return parameters;
  }

  public String getId() {
    return serviceTrans.getContainerObjectId();
  }

  public Boolean hasErrors() {
    return serviceTrans.getErrors() > 0 || genTrans.getErrors() > 0;
  }

  /**
   * Calculate the name of the generated transformation based on the SQL
   *
   * @return the generated name;
   */
  public static String calculateTransname( SQL sql, boolean isService ) {
    StringBuilder sbsql = new StringBuilder( sql.getServiceName() );
    sbsql.append( " - " );
    if ( isService ) {
      sbsql.append( "Service" );
    } else {
      sbsql.append( "SQL" );
    }
    sbsql.append( " - " );
    sbsql.append( sql.getSqlString().hashCode() );

    // Get rid of newlines...
    //
    for ( int i = sbsql.length() - 1; i >= 0; i-- ) {
      if ( sbsql.charAt( i ) == '\n' || sbsql.charAt( i ) == '\r' ) {
        sbsql.setCharAt( i, ' ' );
      }
    }
    return sbsql.toString();
  }

  public void stop() {
    if ( !service.isStreaming() ) {
      if ( serviceTrans.isRunning() ) {
        serviceTrans.stopAll();
      }
      if ( genTrans.isRunning() ) {
        genTrans.stopAll();
      }
    }
  }

  public boolean isStopped() {
    return genTrans.isStopped();
  }

  /**
   * @return the sql
   */
  public SQL getSql() {
    return sql;
  }

  /**
   * @return the resultStepName
   */
  public String getResultStepName() {
    return sqlTransGenerator.getResultStepName();
  }

  public int getRowLimit() {
    return sqlTransGenerator.getRowLimit();
  }

  public int getServiceRowLimit() {
    return sqlTransGenerator.getServiceRowLimit();
  }

  public ListMultimap<ExecutionPoint, Runnable> getListenerMap() {
    return listenerMap;
  }

  /**
   * @author nhudak
   */
  public enum ExecutionPoint {
    PREPARE, OPTIMIZE, READY, START
  }

}
