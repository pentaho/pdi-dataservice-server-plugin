/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2015 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package com.pentaho.di.trans.dataservice.client;

import com.pentaho.di.trans.dataservice.DataServiceExecutor;
import com.pentaho.di.trans.dataservice.DataServiceMeta;
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
import org.pentaho.di.ui.spoon.Spoon;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DataServiceClient implements DataServiceClientService {

  private static Log logger = LogFactory.getLog( DataServiceClient.class );

  AtomicLong dataSize = new AtomicLong( 0L );

  private Repository repository;
  private IMetaStore metaStore;

  @Override public DataInputStream query( String sqlQuery, final int maxRows ) throws SQLException {

    DataInputStream dataInputStream = null;

    try {

      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream( byteArrayOutputStream );

      try {

        List<DataServiceMeta> dataServices = DataServiceMeta.getMetaStoreFactory( metaStore ).getElements();

        final DataServiceExecutor executor = new DataServiceExecutor.Builder( new SQL( sqlQuery ), dataServices ).
            lookupServiceTrans( repository ).
            build();

        dos.writeUTF( executor.getServiceName() );

        dos.writeUTF( DataServiceExecutor.calculateTransname( executor.getSql(), true ) );
        String serviceContainerObjectId = UUID.randomUUID().toString();
        dos.writeUTF( serviceContainerObjectId );
        dos.writeUTF( DataServiceExecutor.calculateTransname( executor.getSql(), false ) );
        String genContainerObjectId = UUID.randomUUID().toString();
        dos.writeUTF( genContainerObjectId );

        final AtomicBoolean firstRow = new AtomicBoolean( true );

        final AtomicInteger rowCounter = new AtomicInteger( 0 );
        final AtomicBoolean wroteRowMeta = new AtomicBoolean( false );

        executor.executeQuery( new RowAdapter() {
          @Override
          public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
            // On the first row, write the metadata...
            //
            try {
              if ( firstRow.get() ) {
                firstRow.set( false );
                rowMeta.writeMeta( dos );
                wroteRowMeta.set( true );
              }
              rowMeta.writeData( dos, row );
              if ( maxRows > 0 && rowCounter.incrementAndGet() > maxRows ) {
                executor.getServiceTrans().stopAll();
              }
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
      List<DataServiceMeta> dataServices = DataServiceMeta.getMetaStoreFactory( metaStore ).getElements();
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
