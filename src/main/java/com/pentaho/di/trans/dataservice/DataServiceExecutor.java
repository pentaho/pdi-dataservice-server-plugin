/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package com.pentaho.di.trans.dataservice;

import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import com.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaAndData;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransAdapter;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepInterface;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class DataServiceExecutor {
  private final Trans serviceTrans;
  private final Trans genTrans;

  private final DataServiceMeta service;
  private final SQL sql;
  private final Map<String, String> parameters;
  private final SqlTransGenerator sqlTransGenerator;

  private DataServiceExecutor( Builder builder ) {
    sql = builder.sql;
    service = builder.service;
    Map<String, String> param = new HashMap<String, String>( builder.parameters );
    param.putAll( getWhereConditionParameters() );
    parameters = Collections.unmodifiableMap( param );
    serviceTrans = builder.serviceTrans;
    sqlTransGenerator = builder.sqlTransGenerator;
    genTrans = builder.genTrans;
  }

  public static class Builder {
    private final SQL sql;
    private final DataServiceMeta service;
    private Trans serviceTrans;
    private Trans genTrans;
    private int rowLimit = 0;
    private Map<String, String> parameters = Collections.emptyMap();
    private LogLevel logLevel;
    private SqlTransGenerator sqlTransGenerator;

    private boolean normalizeConditions = true;
    private boolean prepareExecution = true;
    private boolean enableMetrics = false;

    public Builder( SQL sql, DataServiceMeta service ) {
      this.sql = sql;
      this.service = service;
    }

    public Builder( SQL sql, List<DataServiceMeta> services ) throws KettleException {
      this.sql = sql;
      this.service = findService( services );
    }

    public Builder parameters( Map<String, String> parameters ) {
      this.parameters = parameters;
      return this;
    }

    public Builder rowLimit( int rowLimit ) {
      this.rowLimit = rowLimit;
      return this;
    }

    public Builder logLevel( LogLevel logLevel ) {
      this.logLevel = logLevel;
      return this;
    }

    public Builder serviceTrans( Trans serviceTrans ) {
      this.serviceTrans = serviceTrans;
      return this;
    }

    public Builder serviceTrans( TransMeta serviceTransMeta ) throws KettleException {
      serviceTransMeta.setName( calculateTransname( sql, true ) );
      serviceTransMeta.activateParameters();
      return serviceTrans( new Trans( serviceTransMeta ) );
    }

    public Builder lookupServiceTrans( Repository repository ) throws KettleException {
      TransMeta transMeta;
      if ( service.getName().equals( "dual" ) ) {
        return this;
      }

      if ( !Const.isEmpty( service.getTransFilename() ) ) {
        try {
          // OK, load the meta-data from file...
          //
          // Don't set internal variables: they belong to the parent thread!
          //
          transMeta = new TransMeta( service.getTransFilename(), false );
          transMeta.getLogChannel().logDetailed(
              "Service transformation was loaded from XML file [" + service.getTransFilename() + "]" );
        } catch ( Exception e ) {
          throw new KettleException( "Unable to load service transformation for service '" + sql.getServiceName() + "'",
            e );
        }
      } else {
        try {
          transMeta = service.lookupTransMeta( repository );
          transMeta.getLogChannel().logDetailed(
              "Service transformation was loaded from repository for service [" + service.getName() + "]" );
        } catch ( Exception e ) {
          throw new KettleException( "Unable to load service transformation for service '"
            + sql.getServiceName() + "' from the repository", e );
        }
      }
      return serviceTrans( transMeta );
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
      if ( serviceTrans != null ) {
        serviceFields = serviceTrans.getTransMeta().getStepFields( service.getStepname() );
      } else {
        serviceFields = new RowMeta();
      }

      sql.parse( serviceFields );

      if ( normalizeConditions ) {
        DataServiceExecutor.normalizeConditions( sql, serviceFields );
      }

      if ( sqlTransGenerator == null ) {
        sqlTransGenerator = new SqlTransGenerator( sql, rowLimit );
      }
      if ( genTrans == null ) {
        genTrans = new Trans( sqlTransGenerator.generateTransMeta() );
      }

      DataServiceExecutor dataServiceExecutor = new DataServiceExecutor( this );

      if ( logLevel != null ) {
        dataServiceExecutor.setLogLevel( logLevel );
      }

      genTrans.setGatheringMetrics( enableMetrics );
      if ( serviceTrans != null ) {
        serviceTrans.setGatheringMetrics( enableMetrics );
      }

      if ( prepareExecution ) {
        dataServiceExecutor.prepareExecution();
      }

      return dataServiceExecutor;
    }

    private DataServiceMeta findService( List<DataServiceMeta> serviceMetaList ) throws KettleException {
      String serviceName = sql.getServiceName();
      DataServiceMeta foundService = null;
      if ( StringUtils.isEmpty( serviceName ) || serviceName.equalsIgnoreCase( "dual" ) ) {
        foundService = new DataServiceMeta();
        foundService .setName( "dual" );
        sql.setServiceName( "dual" );
      } else {
        for ( DataServiceMeta service : serviceMetaList ) {
          if ( service.getName().equalsIgnoreCase( serviceName ) ) {
            foundService = service;
          }
        }
      }
      if ( foundService == null ) {
        serviceNotFound();
      }
      return foundService;
    }

    private void serviceNotFound() throws KettleException {
      throw new KettleException( "Unable to find service with name '" + sql.getServiceName() + "' and SQL: " + sql.getSqlString() );
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

  protected static void normalizeConditions( SQL sql, RowMetaInterface fields ) throws KettleStepException {
    ValueMetaResolver resolver = new ValueMetaResolver( fields );
    if ( sql.getWhereCondition() != null && sql.getWhereCondition().getCondition() != null ) {
      convertCondition( sql.getWhereCondition().getCondition(), resolver );
    }
    if ( sql.getHavingCondition() != null && sql.getHavingCondition().getCondition() != null ) {
      convertCondition( sql.getHavingCondition().getCondition(), resolver );
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
    String fieldName = condition.getLeftValuename();
    ValueMetaAndData rhs = condition.getRightExact();
    try {
      // Determine meta and resolve value
      ValueMetaInterface resolvedValueMeta = resolver.getValueMeta( fieldName );
      Object resolvedValue = resolver.getTypedValue( fieldName, rhs.getValueMeta().getType(), rhs.getValueData() );

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
        typedValueStrings[i] = resolvedValueMeta.getString( typedValues[i] );
      }

      // Set new condition in-list (leave meta as string)
      condition.getRightExact().setValueData( StringUtils.join( typedValueStrings, ';' ) );
    } catch ( KettleException e ) {
      // Skip conversion of this condition?
    }
  }

  private void extractConditionParameters( Condition condition, Map<String, String> map ) {

    if ( condition.isAtomic() ) {
      if ( condition.getFunction() == Condition.FUNC_TRUE ) {
        map.put( condition.getLeftValuename(), condition.getRightExactString() );
      }
    } else {
      for ( Condition sub : condition.getChildren() ) {
        extractConditionParameters( sub, map );
      }
    }
  }

  protected void prepareExecution() throws KettleException {
    genTrans.prepareExecution( null );

    if ( !isDual() ) {
      TransMeta serviceTransMeta = getServiceTransMeta();
      for ( Entry<String, String> parameter : parameters.entrySet() ) {
        serviceTransMeta.setParameterValue( parameter.getKey(), parameter.getValue() );
      }
      serviceTrans.prepareExecution( null );
    }
  }

  private Map<String, String> getWhereConditionParameters() {
    // Parameters: see which ones are defined in the SQL
    //
    Map<String, String> conditionParameters = new HashMap<String, String>();
    if ( sql.getWhereCondition() != null ) {
      extractConditionParameters( sql.getWhereCondition().getCondition(), conditionParameters );
    }
    return conditionParameters;
  }


  public void executeQuery( RowListener resultRowListener ) throws KettleException {
    if ( !isDual() ) {
      // Apply Push Down Optimizations
      for ( PushDownOptimizationMeta optimizationMeta : service.getPushDownOptimizationMeta() ) {
        if ( optimizationMeta.isEnabled() ) {
          optimizationMeta.activate( this );
        }
      }

      // This is where we will inject the rows from the service transformation step
      //
      final RowProducer rowProducer = genTrans.addRowProducer( sqlTransGenerator.getInjectorStepName(), 0 );

      // Now connect the 2 transformations with listeners and injector
      //
      StepInterface serviceStep = serviceTrans.findRunThread( service.getStepname() );
      serviceStep.addRowListener( new RowAdapter() {
        @Override
        public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
          // Simply pass along the row to the other transformation (to the Injector step)
          //
          LogChannelInterface log = serviceTrans.getLogChannel();
          try {
            if ( log.isRowLevel() ) {
              log.logRowlevel( "Passing along row: " + rowMeta.getString( row ) );
            }
          } catch ( KettleValueException e ) {
            // Ignore errors
          }

          rowProducer.putRow( rowMeta, row );
        }
      } );

      // Let the other transformation know when there are no more rows
      //
      serviceTrans.addTransListener( new TransAdapter() {
        @Override
        public void transFinished( Trans trans ) throws KettleException {
          rowProducer.finished();
        }
      } );
    }

    // Give back the eventual result rows...
    //
    StepInterface resultStep = genTrans.findRunThread( getResultStepName() );
    resultStep.addRowListener( resultRowListener );

    // Start both transformations
    //
    genTrans.startThreads();
    if ( !isDual() ) {
      serviceTrans.startThreads();
    }
  }

  public void waitUntilFinished() {
    if ( !isDual() ) {
      serviceTrans.waitUntilFinished();
    }
    genTrans.waitUntilFinished();
  }

  /**
   * @return the serviceTransMeta
   */
  public TransMeta getServiceTransMeta() {
    return isDual() ? null : serviceTrans.getTransMeta();
  }

  /**
   * @return the genTransMeta
   */
  public TransMeta getGenTransMeta() {
    return genTrans.getTransMeta();
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
    sbsql.append( sql.getSqlString() );

    // Get rid of newlines...
    //
    for ( int i = sbsql.length() - 1; i >= 0; i-- ) {
      if ( sbsql.charAt( i ) == '\n' || sbsql.charAt( i ) == '\r' ) {
        sbsql.setCharAt( i, ' ' );
      }
    }
    return sbsql.toString();
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

  public boolean isDual() {
    return Const.isEmpty( getServiceName() ) || "dual".equalsIgnoreCase( getServiceName() );
  }
}
