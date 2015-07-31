/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.clients;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.client.DataServiceClientService;
import org.pentaho.di.trans.dataservice.jdbc.ThinServiceInformation;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.metastore.api.IMetaStore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class DataServiceClient implements DataServiceClientService {

  private static Log logger = LogFactory.getLog( DataServiceClient.class );
  private final DataServiceMetaStoreUtil metaStoreUtil;

  private Repository repository;
  private IMetaStore metaStore;

  public DataServiceClient( DataServiceContext context ) {
    this.metaStoreUtil = context.getMetaStoreUtil();
  }

  @Override public DataInputStream query( String sqlQuery, final int maxRows ) throws SQLException {
    DataInputStream dataInputStream = null;

    try {

      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

      try {
        DataServiceExecutor executor = buildExecutor( sqlQuery ).rowLimit( maxRows ).build();

        executor.executeQuery( byteArrayOutputStream ).waitUntilFinished();
      } catch ( Exception e ) {
        throw new SQLException( "Unable to get service information from server", e );
      }

      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( byteArrayOutputStream.toByteArray() );
      dataInputStream = new DataInputStream( byteArrayInputStream );

    } catch ( Exception e ) {
      e.printStackTrace();
    }

    return dataInputStream;
  }

  public DataServiceExecutor.Builder buildExecutor( String sqlQuery ) throws KettleException {
    // Parse SQL
    SQL sql = new SQL( sqlQuery );

    // Locate data service and return a new builder
    DataServiceMeta dataService = findDataService( sql );

    return new DataServiceExecutor.Builder( sql, dataService );
  }

  private DataServiceMeta findDataService( SQL sql ) throws KettleException {
    List<String> dataServiceNames;
    try {
      dataServiceNames = metaStoreUtil.getDataServiceNames( metaStore );
    } catch ( Exception e ) {
      Throwables.propagateIfPossible( e, KettleException.class );
      throw new KettleException( "Unable to get list of data services from MetaStore", e );
    }

    for ( String serviceName : dataServiceNames ) {
      if ( serviceName.equalsIgnoreCase( sql.getServiceName() ) ) {
        try {
          return metaStoreUtil.getDataService( serviceName, repository, metaStore );
        } catch ( Exception e ) {
          Throwables.propagateIfPossible( e, KettleException.class );
          throw new KettleException( "Unable to execute query", e );
        }
      }
    }
    throw new KettleException( "Data service " + sql.getServiceName() + " not found" );
  }

  @Override public List<ThinServiceInformation> getServiceInformation() throws SQLException {
    List<ThinServiceInformation> services = new ArrayList<ThinServiceInformation>();

    for ( DataServiceMeta service : metaStoreUtil.getDataServices( repository, metaStore, logErrors() ) ) {
      TransMeta transMeta = service.getServiceTrans();
      try {
        transMeta.activateParameters();
        RowMetaInterface serviceFields = transMeta.getStepFields( service.getStepname() );
        ThinServiceInformation serviceInformation = new ThinServiceInformation( service.getName(), serviceFields );
        services.add( serviceInformation );
      } catch ( Exception e ) {
        logger.warn( MessageFormat.format( "Unable to get fields for service {0}, transformation: {1}",
          service.getName(), transMeta.getName() ) );
      }
    }

    return services;
  }

  private Function<Exception, Void> logErrors() {
    return new Function<Exception, Void>() {
      @Override public Void apply( Exception e ) {
        logger.warn( "Failure loading data service", e );
        return null;
      }
    };
  }

  public Repository getRepository() {
    return repository;
  }

  public void setRepository( Repository repository ) {
    this.repository = repository;
  }

  public IMetaStore getMetaStore() {
    return metaStore;
  }

  public void setMetaStore( IMetaStore metaStore ) {
    this.metaStore = metaStore;
  }
}
