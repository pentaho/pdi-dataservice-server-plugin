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
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.client.DataServiceClientService;
import org.pentaho.di.trans.dataservice.jdbc.ThinServiceInformation;
import org.pentaho.di.trans.dataservice.serialization.DataServiceFactory;
import org.pentaho.metastore.api.IMetaStore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;

public class DataServiceClient implements DataServiceClientService {
  private final DataServiceFactory factory;

  public static final String DUMMY_TABLE_NAME = "dual";

  public DataServiceClient( DataServiceFactory dataServiceFactory ) {
    this.factory = dataServiceFactory;
  }

  public DataServiceFactory getFactory() {
    return factory;
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
        DataServiceExecutor executor = factory.createBuilder( sql )
          .rowLimit( maxRows )
          .build();
        executor
          .executeQuery( new DataOutputStream( byteArrayOutputStream ) )
          .waitUntilFinished();

        if ( executor.hasErrors() ) {
          throw new SQLException( "There were errors in the execution of this SQL." );
        }
      }

      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( byteArrayOutputStream.toByteArray() );
      dataInputStream = new DataInputStream( byteArrayInputStream );

    } catch ( Exception e ) {
      Throwables.propagateIfPossible( e, SQLException.class );
      throw new SQLException( e );
    }

    return dataInputStream;
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

  @Override public List<ThinServiceInformation> getServiceInformation() throws SQLException {
    List<ThinServiceInformation> services = Lists.newArrayList();

    for ( DataServiceMeta service : factory.getDataServices( logErrors() ) ) {
      TransMeta transMeta = service.getServiceTrans();
      try {
        transMeta.activateParameters();
        RowMetaInterface serviceFields = transMeta.getStepFields( service.getStepname() );
        ThinServiceInformation serviceInformation = new ThinServiceInformation( service.getName(), serviceFields );
        services.add( serviceInformation );
      } catch ( Exception e ) {
        String message = MessageFormat.format( "Unable to get fields for service {0}, transformation: {1}",
          service.getName(), transMeta.getName() );
        factory.getLogChannel().logError( message, e );
      }
    }

    return services;
  }

  private Function<Exception, Void> logErrors() {
    return factory.logErrors( "Unable to retrieve data service" );
  }

  /**
   * @deprecated Property is unused. See {@link DataServiceClientService#setRepository(Repository)}
   */
  @Deprecated
  public void setRepository( Repository repository ) {
  }

  /**
   * @deprecated Property is unused. See {@link DataServiceClientService#setMetaStore(IMetaStore)}
   */
  @Deprecated
  public void setMetaStore( IMetaStore metaStore ) {
  }
}
