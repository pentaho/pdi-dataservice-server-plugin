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
import com.google.common.collect.Lists;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
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
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;

public class DataServiceClient implements DataServiceClientService {
  private final DataServiceMetaStoreUtil metaStoreUtil;
  private final DataServiceContext context;

  private Repository repository;
  private IMetaStore metaStore;

  public static final String DUMMY_TABLE_NAME = "dual";

  public DataServiceClient( DataServiceContext context ) {
    this.metaStoreUtil = context.getMetaStoreUtil();
    this.context = context;
  }

  @Override public DataInputStream query( String sqlQuery, final int maxRows ) throws SQLException {
    DataInputStream dataInputStream;

    try {

      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

      SQL sql = new SQL( sqlQuery );
      if ( sql.getServiceName() == null || sql.getServiceName().equals( DUMMY_TABLE_NAME ) ) {
        // Support for SELECT 1 and SELECT 1 FROM dual
        DataOutputStream dos = new DataOutputStream( byteArrayOutputStream );
        writeDummyRow( sql, dos );
      } else {
        DataServiceExecutor executor = buildExecutor( sql )
          .rowLimit( maxRows )
          .build();
        executor
          .executeQuery( new DataOutputStream( byteArrayOutputStream ) )
          .waitUntilFinished();
      }

      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( byteArrayOutputStream.toByteArray() );
      dataInputStream = new DataInputStream( byteArrayInputStream );

    } catch ( Exception e ) {
      Throwables.propagateIfPossible( e, SQLException.class );
      throw new SQLException( e );
    }

    return dataInputStream;
  }

  public DataServiceExecutor.Builder buildExecutor( SQL sql ) throws KettleException {
    // Locate data service and return a new builder
    DataServiceMeta dataService = findDataService( sql );

    return context.createBuilder( sql, dataService );
  }

  public void writeDummyRow( SQL sql, DataOutputStream dos ) throws Exception {
    sql.setServiceName( DUMMY_TABLE_NAME );

    DataServiceExecutor.writeMetadata( dos, new String[] { DUMMY_TABLE_NAME, "", "", "", "" } );

    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "DUMMY" ) );
    rowMeta.writeMeta( dos );

    Object[] row = new Object[] { "x" };
    rowMeta.writeData( dos, row );
  }

  private DataServiceMeta findDataService( SQL sql ) throws KettleException {
    try {
      return metaStoreUtil.getDataService( sql.getServiceName(), repository, metaStore );
    } catch ( Exception e ) {
      Throwables.propagateIfPossible( e, KettleException.class );
      throw new KettleException( "Unable to locate data service", e );
    }
  }

  @Override public List<ThinServiceInformation> getServiceInformation() throws SQLException {
    List<ThinServiceInformation> services = Lists.newArrayList();

    for ( DataServiceMeta service : metaStoreUtil.getDataServices( repository, metaStore, logErrors() ) ) {
      TransMeta transMeta = service.getServiceTrans();
      try {
        transMeta.activateParameters();
        RowMetaInterface serviceFields = transMeta.getStepFields( service.getStepname() );
        ThinServiceInformation serviceInformation = new ThinServiceInformation( service.getName(), serviceFields );
        services.add( serviceInformation );
      } catch ( Exception e ) {
        String message = MessageFormat.format( "Unable to get fields for service {0}, transformation: {1}",
          service.getName(), transMeta.getName() );
        context.getLogChannel().logError( message, e );
      }
    }

    return services;
  }

  private Function<Exception, Void> logErrors() {
    return metaStoreUtil.logErrors( "Unable to retrieve data service" );
  }

  public void setRepository( Repository repository ) {
    this.repository = repository;
  }

  public void setMetaStore( IMetaStore metaStore ) {
    this.metaStore = metaStore;
  }

  public FileOutputStream getDebugFileOutputStream( String filename ) throws FileNotFoundException {
    return new FileOutputStream( filename );
  }
}
