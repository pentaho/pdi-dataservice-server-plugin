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

package org.pentaho.di.trans.dataservice.client;

import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.DataServiceMetaStoreUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.client.DataServiceClientService;
import org.pentaho.di.trans.dataservice.jdbc.ThinServiceInformation;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.metastore.api.IMetaStore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class DataServiceClient implements DataServiceClientService {

  private static Log logger = LogFactory.getLog( DataServiceClient.class );
  private final DataServiceMetaStoreUtil metaStoreUtil;

  AtomicLong dataSize = new AtomicLong( 0L );

  private Repository repository;
  private IMetaStore metaStore;

  public DataServiceClient( DataServiceMetaStoreUtil metaStoreUtil ) {
    this.metaStoreUtil = metaStoreUtil;
  }

  @Override public DataInputStream query( String sqlQuery, final int maxRows ) throws SQLException {

    DataInputStream dataInputStream = null;

    try {

      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream( byteArrayOutputStream );

      try {

        List<DataServiceMeta> dataServices = metaStoreUtil.getMetaStoreFactory( metaStore )
          .getElements();

        final DataServiceExecutor executor = new DataServiceExecutor.Builder( new SQL( sqlQuery ), dataServices )
          .lookupServiceTrans( repository )
          .rowLimit( maxRows )
          .build();

        dos.writeUTF( executor.getServiceName() );

        dos.writeUTF( DataServiceExecutor.calculateTransname( executor.getSql(), true ) );
        String serviceContainerObjectId = UUID.randomUUID().toString();
        dos.writeUTF( serviceContainerObjectId );
        dos.writeUTF( DataServiceExecutor.calculateTransname( executor.getSql(), false ) );
        String genContainerObjectId = UUID.randomUUID().toString();
        dos.writeUTF( genContainerObjectId );

        final AtomicBoolean firstRow = new AtomicBoolean( true );

        final AtomicBoolean wroteRowMeta = new AtomicBoolean( false );

        executor.executeQuery( new RowAdapter() {
          @Override
          public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
            // On the first row, write the metadata...
            //
            try {
              if ( firstRow.compareAndSet( true, false ) ) {
                rowMeta.writeMeta( dos );
                wroteRowMeta.set( true );
              }
              rowMeta.writeData( dos, row );
              dataSize.set( dos.size() );
            } catch ( Exception e ) {
              if ( !executor.getServiceTrans().isStopped() ) {
                throw new KettleStepException( e );
              }
            }
          }
        } );

        executor.waitUntilFinished();

        if ( !wroteRowMeta.get() ) {
          RowMetaInterface stepFields = executor.getGenTransMeta().getStepFields( executor.getResultStepName() );
          stepFields.writeMeta( dos );
        }

      } catch ( Exception e ) {
        throw new SQLException( "Unable to get service information from server", e );
      } finally {
        System.out.println( "bytes written: " + dataSize );
      }

      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( byteArrayOutputStream.toByteArray() );
      dataInputStream = new DataInputStream( byteArrayInputStream );

    } catch ( Exception e ) {
      e.printStackTrace();
    }

    return dataInputStream;
  }

  @Override public List<ThinServiceInformation> getServiceInformation() throws SQLException {

    List<ThinServiceInformation> services = new ArrayList<ThinServiceInformation>();

    try {
      List<DataServiceMeta> dataServices = metaStoreUtil.getMetaStoreFactory( metaStore ).getElements();
      for ( DataServiceMeta service : dataServices ) {
        RowMetaInterface serviceFields = null;
        try {
          TransMeta transMeta = service.lookupTransMeta( repository );
          serviceFields = transMeta.getStepFields( service.getStepname() );
        } catch ( Exception e ) {
          logger.error( "Unable to get fields for service " + service.getName() + ", transformation: " + service
              .getTransFilename() );
        }

        ThinServiceInformation serviceInformation = new ThinServiceInformation( service.getName(), serviceFields );
        services.add( serviceInformation );
      }
    } catch ( Exception e ) {
      throw new SQLException( "Unable to get service information from server", e );
    }

    return services;
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
