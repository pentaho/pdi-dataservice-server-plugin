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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.MultimapBuilder;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import io.reactivex.subjects.PublishSubject;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
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
import org.pentaho.di.trans.dataservice.optimization.OptimizationImpactInfo;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
import org.pentaho.di.trans.dataservice.streaming.StreamServiceKey;
import org.pentaho.di.trans.dataservice.streaming.WindowParametersHelper;
import org.pentaho.di.trans.dataservice.streaming.execution.StreamingGeneratedTransExecution;
import org.pentaho.di.trans.dataservice.streaming.execution.StreamingServiceTransExecutor;
import org.pentaho.di.trans.dataservice.utils.DataServiceConstants;
import org.pentaho.di.trans.dataservice.utils.KettleUtils;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.metastore.api.IMetaStore;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public class DataServiceExecutor {

  private static final Log logger = LogFactory.getLog( DataServiceExecutor.class );

  private static final Class<?> PKG = DataServiceExecutor.class;

  private final Trans serviceTrans;
  private final Trans genTrans;

  private final DataServiceMeta service;
  private final SQL sql;

  private final Map<String, String> parameters;
  private final SqlTransGenerator sqlTransGenerator;
  private final ListMultimap<ExecutionPoint, Runnable> listenerMap;
  private final Context context;

  private IDataServiceClientService.StreamingMode windowMode;
  private long windowSize;
  private long windowEvery;
  private long windowLimit;
  private StreamServiceKey streamServiceKey;

  private final AtomicBoolean genTransformationPushBasedIsFinished = new AtomicBoolean( false );
  private final AtomicBoolean transListenerFinishTransAdded = new AtomicBoolean( false );

  private DataServiceExecutor( Builder builder ) {
    sql = builder.sql;
    service = builder.service;
    parameters = builder.parameters;
    serviceTrans = builder.serviceTrans;
    sqlTransGenerator = builder.sqlTransGenerator;
    genTrans = builder.genTrans;
    context = builder.context;
    windowMode = builder.windowMode;
    windowSize = builder.windowSize;
    windowEvery = builder.windowEvery;
    windowLimit = builder.windowLimit;
    streamServiceKey = builder.streamServiceKey;

    listenerMap = MultimapBuilder.enumKeys( ExecutionPoint.class ).linkedListValues().build();
  }

  public static class Builder {
    private final SQL sql;
    private final DataServiceMeta service;
    private final Context context;
    private Trans serviceTrans;
    private Trans genTrans;
    private int rowLimit = 0;
    private long timeLimit = 0;
    private IDataServiceClientService.StreamingMode windowMode;
    private long windowSize = 0;
    private long windowEvery = 0;
    private long windowLimit = 0;
    private Map<String, String> parameters = new HashMap<>();
    private LogLevel logLevel;
    private SqlTransGenerator sqlTransGenerator;
    private StreamServiceKey streamServiceKey;

    private boolean normalizeConditions = true;
    private boolean prepareExecution = true;
    private boolean enableMetrics = false;
    private IMetaStore metastore;
    private BiConsumer<String, TransMeta> transMutator =
      ( stepName, transMeta ) -> TransMutators.disableAllUnrelatedHops( stepName, transMeta, true );

    private KettleUtils kettleUtils = KettleUtils.getInstance();

    public Builder( SQL sql, DataServiceMeta service, Context context ) {
      this.sql = Preconditions.checkNotNull( sql, "SQL must not be null." );
      this.service = Preconditions.checkNotNull( service, "Service must not be null." );
      this.context = context;
    }

    public Builder parameters( Map<String, String> parameters ) {
      this.parameters = Maps.newHashMap( parameters );
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

      if ( service.isStreaming() && windowMode == null ) {
        throw new KettleException(
            BaseMessages.getString( PKG, "DataServiceExecutor.Error.WindowModeMandatory",
                sql.getServiceName(), service.getName() ) );
      }

      if ( sql.getServiceName() == null || !sql.getServiceName().equals( service.getName() ) ) {
        throw new KettleException(
            BaseMessages.getString( PKG, "DataServiceExecutor.Error.TableNameAndDataServiceNameDifferent",
                sql.getServiceName(), service.getName() ) );
      }

      // Sets the service transformation fields
      if ( service.getServiceTrans() != null ) {
        serviceFields = service.getServiceTrans().getStepFields( service.getStepname() );
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

      // Check if there is already a serviceTransformation in the context
      if ( service.isStreaming() ) {
        this.streamServiceKey = getStreamingServiceKey();
        StreamingServiceTransExecutor serviceTransExecutor = context.getServiceTransExecutor( streamServiceKey );

        if ( serviceTransExecutor != null
            && !serviceTransExecutor.getServiceTrans().getTransMeta().getModifiedDate()
            .equals( service.getServiceTrans().getModifiedDate() ) ) {
          context.removeServiceTransExecutor( serviceTransExecutor.getKey() );
          serviceTransExecutor.stopAll();
          serviceTransExecutor = null;
        }

        // Gets service execution from context
        if ( serviceTransExecutor == null ) {
          if ( serviceTrans == null && service.getServiceTrans() != null ) {
            serviceTrans( service.getServiceTrans() );
          }

          if ( serviceTrans != null ) {
            int windowMaxRowLimit = (int) getStreamingLimit( rowLimit, service.getRowLimit(), getKettleRowLimit(),
                DataServiceConstants.ROW_LIMIT_DEFAULT );
            long windowMaxTimeLimit = getStreamingLimit( timeLimit, service.getTimeLimit(), getKettleTimeLimit(),
                DataServiceConstants.TIME_LIMIT_DEFAULT );

            serviceTransExecutor =
                new StreamingServiceTransExecutor( streamServiceKey, serviceTrans, service.getStepname(),
                windowMaxRowLimit, windowMaxTimeLimit, context );
            context.addServiceTransExecutor( serviceTransExecutor );
          }
        } else {
          prepareExecution = false;
          serviceTrans( serviceTransExecutor.getServiceTrans() );
        }
      } else if ( serviceTrans == null && service.getServiceTrans() != null ) {
        serviceTrans( service.getServiceTrans() );
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

      this.parameters.putAll( getWhereConditionParameters() );

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

    private StreamServiceKey getStreamingServiceKey() {
      // Apply Push Down Optimizations
      List<OptimizationImpactInfo> optimizationImpactList = new ArrayList<>();

      if ( serviceTrans == null ) {
        this.serviceTrans( service.getServiceTrans() );
      }

      DataServiceExecutor transientDataServiceExecutor = new DataServiceExecutor( this );
      for ( PushDownOptimizationMeta optimizationMeta : service.getPushDownOptimizationMeta() ) {
        if ( optimizationMeta.isEnabled() ) {
          optimizationImpactList.add( optimizationMeta.preview( transientDataServiceExecutor ) );
        }
      }

      return StreamServiceKey.create( service.getName(), parameters, optimizationImpactList );
    }

    /**
     * Gets the streaming dataservice max limit for a window.
     *
     * @param userLimit    - The user limit.
     * @param serviceLimit - The metadata limit.
     * @param kettleLimit  - The kettle limit.
     * @param defaultLimit - The default limit.
     *
     * @return The streaming max limit.
     *
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

    @VisibleForTesting
    protected int getKettleRowLimit() throws KettleException {
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

    @VisibleForTesting
    protected long getKettleTimeLimit() throws KettleException {
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

    private Map<String, String> getWhereConditionParameters() {
      // Parameters: see which ones are defined in the SQL
      //
      Map<String, String> conditionParameters = new HashMap<>();
      if ( sql.getWhereCondition() != null ) {
        extractConditionParameters( sql.getWhereCondition().getCondition(), conditionParameters );
      }
      return conditionParameters;
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
      String[] typedValueStrings = new String[ typedValues.length ];
      for ( int i = 0; i < typedValues.length; i++ ) {
        typedValueStrings[ i ] = resolvedValueMeta.getCompatibleString( typedValues[ i ] );
      }

      // Set new condition in-list (leave meta as string)
      condition.getRightExact().setValueData( StringUtils.join( typedValueStrings, ';' ) );
    } catch ( KettleException e ) {
      // Skip conversion of this condition?
    }
  }

  protected void prepareExecution() throws KettleException {
    // Setup executor with streaming execution plan
    ImmutableMultimap.Builder<ExecutionPoint, Runnable> builder = ImmutableMultimap.builder();

    builder.putAll( ExecutionPoint.PREPARE,
        new CopyParameters( parameters, serviceTrans )
    );

    if ( !service.isStreaming() ) {
      builder.putAll( ExecutionPoint.PREPARE,
          new PrepareExecution( serviceTrans ),
          new PrepareExecution( genTrans ) );

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

    genTransformationPushBasedIsFinished.set( false );

    // When done, check if no row metadata was written.  The client is still going to expect it...
    // Since we know it, we'll pass it.
    if ( transListenerFinishTransAdded.compareAndSet( false, true ) ) {
      getGenTrans().addTransListener( new TransAdapter() {
        @Override
        public void transFinished( Trans trans ) throws KettleException {
          writeMeta( trans, dos, rowMetaWritten );
        }
      } );
    }

    // Now execute the query transformation(s) and pass the data to the output stream, using a reactivex consumer
    final PublishSubject<RowMetaAndData> consumer = PublishSubject.create();
    Disposable[] disposableWrapper = new Disposable[ 1 ];
    consumer
      .doOnError( t -> {
        logger.error( "Error consuming data from the generated transformation into the consumer", t );
        wrapupConsumerResources( consumer );
      } )
      .doOnComplete( () -> {
        //In cases where there are now rows generated, it may end without having written the
        //metadata, and in that case the pipes are closed and an exception may rise (pipe close)
        writeMeta( getGenTrans(), dos, rowMetaWritten );

        genTransformationPushBasedIsFinished.set( true );
        if ( disposableWrapper[ 0 ] != null ) {
          disposableWrapper[ 0 ].dispose();
        }
      } )
      .doOnSubscribe( disposable -> disposableWrapper[ 0 ] = disposable )
      .subscribe( rowMetaAndData -> {
        try {
          RowMetaInterface rowMetaInterface = rowMetaAndData.getRowMeta();
          writeMeta( rowMetaInterface, dos, rowMetaWritten );
          rowMetaInterface.writeData( dos, rowMetaAndData.getData() );
        } catch ( Exception e ) {
          if ( !getServiceTrans().isStopped() ) {
            throw new KettleStepException( e );
          }
        }
      } );

    return executeQuery( consumer );
  }

  /**
   * Stub method to call the writeMeta with the RowMetaInterface.
   * @param generatedTransformation
   * @param dos
   * @param rowMetaWritten
   */
  private void writeMeta( Trans generatedTransformation, DataOutputStream dos, AtomicBoolean rowMetaWritten ) {
    try {
      if ( !rowMetaWritten.get() ) {
        RowMetaInterface stepFields = generatedTransformation.getTransMeta().getStepFields( getResultStepName() );
        writeMeta( stepFields, dos, rowMetaWritten );
      }
    } catch ( Exception e ) {
      throw new RuntimeException( e );
    }
  }

  /**
   * Writes the meta into the received output stream.
   * Checks if the meta was already written before doing it.
   * @param rowMetaInterface
   * @param dos
   * @param rowMetaWritten
   */
  private void writeMeta( RowMetaInterface rowMetaInterface, DataOutputStream dos, AtomicBoolean rowMetaWritten )
    throws KettleFileException {
    if ( rowMetaWritten.compareAndSet( false, true ) ) {
      rowMetaInterface.writeMeta( dos );
    }
  }

  /**
   * Wrap up consumer resources and removes it from the consumers list.
   * @param consumer the consumer that we want to clean up.
   */
  @VisibleForTesting
  protected void wrapupConsumerResources( Observer<RowMetaAndData> consumer ) {
    consumer.onComplete();

    StreamingServiceTransExecutor serviceTransExecutor = context.getServiceTransExecutor( streamServiceKey );
    String streamingGenTransCacheKey = getStreamingGenTransCacheKey();
    serviceTransExecutor.clearCacheByKey( streamingGenTransCacheKey );

    context.removeServiceTransExecutor( serviceTransExecutor.getKey().getDataServiceId() );

    genTransformationPushBasedIsFinished.set( true );
  }

  public DataServiceExecutor executeQuery( Observer<RowMetaAndData> consumer ) {
    if ( service.isStreaming() ) {
      //Builds an accumulator to gather all the rows and then dispatch them to the row-by-row consumer
      final Disposable[] disposableWrapper = new Disposable[ 1 ];

      PublishSubject<List<RowMetaAndData>> accumulator = PublishSubject.create();
      accumulator.doOnNext( rowMetaAndDataList -> {
        rowMetaAndDataList.stream().forEach( consumer::onNext );
        // since we are polling, after a window is received we complete the consumer
        consumer.onComplete();
        accumulator.onComplete();
        if ( disposableWrapper[ 0 ] != null ) {
          disposableWrapper[ 0 ].dispose();
        }
      } ).doOnSubscribe( disposable -> disposableWrapper[ 0 ] = disposable ).subscribe();

      return executeStreamingQuery( accumulator, true );
    } else {
      return executeDefaultQuery( consumer );
    }
  }

  /**
   * Executes a streaming push query. If the pollingMode is passed as true, then the resulting query is going to return a single
   * window, and the consumer is not kept as an active consumer in the consumer list.
   * This method is synchronized to make sure that no duplicate entries are created if we have two, or more, executions running in parallel for the same
   * dataservice and query.
   * @param streamingConsumer
   * @param pollingMode True, if the query is to be executed in polling mode
   * @return
   */
  public DataServiceExecutor executeStreamingQuery( final Observer<List<RowMetaAndData>> streamingConsumer, boolean pollingMode ) {

    String streamingGenTransCacheKey = getStreamingGenTransCacheKey();

    if ( streamingGenTransCacheKey == null ) {
      return null;
    }

    //Try to fetch the streaming generated transformation execution from cache (synchronize to avoid adding duplicates)
    synchronized ( context ) {
      StreamingGeneratedTransExecution streamingGenTransFromCache = context.getStreamingGeneratedTransExecution( streamingGenTransCacheKey );
      if ( streamingGenTransFromCache == null ) {
        StreamingGeneratedTransExecution streamWiring =
          new StreamingGeneratedTransExecution( context.getServiceTransExecutor( streamServiceKey ),
            genTrans, streamingConsumer, pollingMode, sqlTransGenerator.getInjectorStepName(),
            sqlTransGenerator.getResultStepName(),
            sqlTransGenerator.getSql().getSqlString(), windowMode, windowSize, windowEvery, windowLimit,
            streamingGenTransCacheKey );

        serviceTrans.addTransListener( new TransAdapter() {
          @Override
          public void transFinished( Trans trans ) throws KettleException {
            //When the service transformation stops we should remove the service stream from the cache
            context.removeServiceTransExecutor( streamServiceKey );
          }
        } );

        listenerMap.get( ExecutionPoint.READY ).add( streamWiring );

        context.addStreamingGeneratedTransExecution( streamingGenTransCacheKey, streamWiring );

        return executeQuery();

      } else {
        //Since we don't create a service transformation executor we need to touch the current one so that
        //it is kept in cache
        context.getServiceTransExecutor( streamServiceKey ).touchServiceListener( streamingGenTransCacheKey );

        //There is already a streaming generated transformation execution in cache, so we just register the consumer as
        //one more consumer for that generated transformation
        streamingGenTransFromCache.addNewRowConsumer( streamingConsumer, pollingMode );
      }
    }

    return this;
  }

  public String getStreamingGenTransCacheKey() {
    StreamingServiceTransExecutor serviceExecutor = context.getServiceTransExecutor( streamServiceKey );
    return WindowParametersHelper.getCacheKey( sqlTransGenerator.getSql().getSqlString(),
      windowMode, windowSize, windowEvery, (int) serviceExecutor.getWindowMaxRowLimit(), serviceExecutor.getWindowMaxTimeLimit(), windowLimit, serviceExecutor.getKey().hashCode() );
  }

  public DataServiceExecutor executeDefaultQuery( final Observer<RowMetaAndData> consumer ) {
    listenerMap.get( ExecutionPoint.READY ).add( new Runnable() {
      @Override
      public void run() {
        // Give back the eventual result rows...
        StepInterface resultStep = genTrans.findRunThread( getResultStepName() );

        if ( resultStep != null ) {
          resultStep.addRowListener( new RowAdapter() {
            @Override
            public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
              consumer.onNext( new RowMetaAndData( rowMeta, row ) );
            }

            @Override
            public void errorRowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
              consumer.onNext( new RowMetaAndData( rowMeta, row ) );
            }
          } );

          //signal the end of the accumulator when the transformation finishes
          genTrans.addTransListener( new TransAdapter() {
            @Override
            public void transFinished( Trans trans ) throws KettleException {
              consumer.onComplete();
            }
          } );
        }
      }
    } );
    return executeQuery();
  }

  public DataServiceExecutor executeQuery() {
    // Apply Push Down Optimizations
    for ( PushDownOptimizationMeta optimizationMeta : service.getPushDownOptimizationMeta() ) {
      if ( optimizationMeta.isEnabled() ) {
        optimizationMeta.activate( this );
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
      genTrans.waitUntilFinished();
    } else {
      try {
        // we need to actively wait for the genTrans to finish, because otherwise it will return on the
        // waitUntilFinished since it didn't had time to start
        while ( !genTrans.isFinishedOrStopped() && !genTransformationPushBasedIsFinished.get() ) {
          Thread.sleep( 50 );
        }

      } catch ( InterruptedException e ) {
        throw new RuntimeException( e );
      }
    }
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

  public void stop( boolean stopTrans ) {
    if ( stopTrans || !service.isStreaming() ) {
      if ( serviceTrans.isRunning() ) {
        serviceTrans.stopAll();
      }
      if ( genTrans.isRunning() ) {
        genTrans.stopAll();
      }

      if ( service.isStreaming() ) {
        this.genTransformationPushBasedIsFinished.set( true );

        StreamingServiceTransExecutor serviceExecutor = context.getServiceTransExecutor( streamServiceKey );
        if ( serviceExecutor != null ) {
          String streamingGenTransCacheKey = getStreamingGenTransCacheKey();
          context.removeStreamingGeneratedTransExecution( streamingGenTransCacheKey );
          serviceExecutor.clearCacheByKey( streamingGenTransCacheKey );
        }
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
