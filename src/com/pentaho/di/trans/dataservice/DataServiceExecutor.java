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
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransAdapter;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.sql.SqlTransMeta;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepInterface;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class DataServiceExecutor {
  private Trans serviceTrans;
  private Trans genTrans;

  private List<DataServiceMeta> services;
  private String serviceName;
  private DataServiceMeta service;
  private SQL sql;
  private int rowLimit;
  private Map<String, String> parameters;
  private SqlTransMeta sqlTransGenerator;
  private RowMetaInterface serviceFields;

  // Initialize empty without prepareExecution
  protected DataServiceExecutor() {
    parameters = Collections.emptyMap();
  }

  public DataServiceExecutor( String sqlQuery, List<DataServiceMeta> services, Map<String, String> parameters,
                              TransMeta transMeta, int rowLimit, LogLevel logLevel ) throws KettleException {
    this( sqlQuery, services, parameters, rowLimit );
    if ( !isDual() ) {
      initServiceTrans( transMeta );
    }
    initGenTrans();
    setLogLevel( logLevel );
    prepareExecution();
  }

  private void setLogLevel( LogLevel logLevel ) {
    if ( serviceTrans != null ) {
      serviceTrans.setLogLevel( logLevel );
    }
    if ( genTrans != null ) {
      genTrans.setLogLevel( logLevel );
    }
  }

  /**
   * @param sqlQuery   User SQL query
   * @param services   Available services
   * @param parameters Connection trans parameters
   * @param repository Repository to search for transformation
   * @throws KettleException
   */
  public DataServiceExecutor( String sqlQuery, List<DataServiceMeta> services, Map<String, String> parameters,
                              Repository repository, int rowLimit ) throws KettleException {
   this( sqlQuery, services, parameters, rowLimit );
    if ( !isDual() ) {
      initServiceTrans( loadTransMeta( repository ) );
    }
    initGenTrans();
    prepareExecution();
  }

  private DataServiceExecutor( String sqlQuery, List<DataServiceMeta> services,
                              Map<String, String> parameters,
                              int rowLimit ) throws KettleException {
    this.services = services;
    this.parameters = parameters;
    this.rowLimit = rowLimit;
    this.sql = new SQL( sqlQuery );
    this.serviceName = sql.getServiceName();
    service = findService( getServiceName() );
  }

  private void initServiceTrans( TransMeta serviceTransMeta ) throws KettleException {
    if ( service == null ) {
      throw new KettleException( "Unable to find service with name '" + getServiceName() + "' and SQL: " + sql.getSqlString() );
    }
    serviceTransMeta.setName( calculateTransname( sql, true ) );
    serviceTransMeta.activateParameters();
    serviceFields = serviceTransMeta.getStepFields( service.getStepname() );
    serviceTrans = new Trans( serviceTransMeta );
  }

  private void initGenTrans() throws KettleException {
    sql.parse( serviceFields );
    sqlTransGenerator = new SqlTransMeta( sql, rowLimit );
    TransMeta genTransMeta = sqlTransGenerator.generateTransMeta();
    genTrans = new Trans( genTransMeta );
  }

  private DataServiceMeta findService( String name ) {
    DataServiceMeta service;
    if ( isDual() ) {
      service = new DataServiceMeta();
      service.setName( "dual" );
      sql.setServiceName( "dual" );
      serviceFields = new RowMeta(); // nothing to report from dual
      return service;
    }
    for ( DataServiceMeta s : services ) {
      if ( s.getName().equalsIgnoreCase( name ) ) {
        return s;
      }
    }
    return null;
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
      // Parameters: see which ones are defined in the SQL
      //
      Map<String, String> conditionParameters = new HashMap<String, String>();
      if ( sql.getWhereCondition() != null ) {
        extractConditionParameters( sql.getWhereCondition().getCondition(), conditionParameters );
      }
      parameters.putAll( conditionParameters ); // overwrite the defaults for this query

      TransMeta serviceTransMeta = getServiceTransMeta();
      for ( Entry<String, String> parameter : parameters.entrySet() ) {
        serviceTransMeta.setParameterValue( parameter.getKey(), parameter.getValue() );
      }
      serviceTrans.prepareExecution( null );
    }
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

  private TransMeta loadTransMeta( Repository repository ) throws KettleException {
    TransMeta transMeta;

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
        throw new KettleException( "Unable to load service transformation for service '" + getServiceName() + "'", e );
      }
    } else {
      try {
        StringObjectId objectId = new StringObjectId( service.getTransObjectId() );
        transMeta = repository.loadTransformation( objectId, null );
        transMeta.getLogChannel().logDetailed(
          "Service transformation was loaded from repository for service [" + service.getName() + "]" );
      } catch ( Exception e ) {
        throw new KettleException( "Unable to load service transformation for service '"
          + getServiceName() + "' from the repository", e );
      }
    }
    return transMeta;
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
    return serviceName;
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

  public void setSql( SQL sql ) {
    this.sql = sql;
  }

  public StepInterface getServiceStep() {
    return getServiceTrans().findRunThread( service.getStepname() );
  }

  /**
   * @return the resultStepName
   */
  public String getResultStepName() {
    return sqlTransGenerator.getResultStepName();
  }

  public void setGenTrans( Trans genTrans ) {
    this.genTrans = genTrans;
  }

  public void setServiceTrans( Trans serviceTrans ) {
    this.serviceTrans = serviceTrans;
  }

  public void setService( DataServiceMeta service ) {
    this.service = service;
  }

  public void setSqlTransMeta( SqlTransMeta sqlTransMeta ) {
    this.sqlTransGenerator = sqlTransMeta;
  }

  public boolean isDual() {
    return Const.isEmpty( getServiceName() ) || "dual".equalsIgnoreCase( getServiceName() );
  }


  public void setServiceName( String serviceName ) {
    this.serviceName = serviceName;
  }
}
