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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.jdbc.ThinServiceInformation;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * Created by bmorrise on 9/30/15.
 */
@RunWith( MockitoJUnitRunner.class )
public class DataServiceClientTest {

  private static final String DUAL_TABLE_NAME = "dual";
  private static final String TEST_DUMMY_SQL_QUERY = "SELECT 1";
  private static final String TEST_SQL_QUERY = "SELECT * FROM dataservice_test";
  private static final String SERVICE_NAME = "dataservice_test";
  private static final int MAX_ROWS = 100;

  @Mock
  private TransMeta trans;

  @Mock
  private Repository repository;

  @Mock
  private IMetaStore metaStore;

  @Mock
  private DataServiceMeta dataServiceMeta;

  @Mock
  private DataServiceExecutor.Builder builder;

  @Mock
  private DataServiceContext context;

  @Mock
  private DataServiceMetaStoreUtil metaStoreUtil;

  @Mock
  private DataServiceExecutor executor;

  @Mock
  private SQL sql;

  @Mock
  private RowMetaInterface rowMetaInterface;

  private DataServiceClient dataServiceClient;

  @Before
  public void setUp() throws Exception {
    when( context.getMetaStoreUtil() ).thenReturn( metaStoreUtil );
    when( context.createBuilder( any( SQL.class ), any( DataServiceMeta.class ) ) ).thenReturn( builder );
    when( builder.rowLimit( MAX_ROWS ) ).thenReturn( builder );
    when( builder.build() ).thenReturn( executor );
    when( executor.executeQuery( any( ByteArrayOutputStream.class ) ) ).thenReturn( executor );
    when( dataServiceMeta.getServiceTrans() ).thenReturn( trans );

    doNothing().when( executor ).waitUntilFinished();

    dataServiceClient = new DataServiceClient( context );
    dataServiceClient.setMetaStore( metaStore );
    dataServiceClient.setRepository( repository );
  }

  @Test
  public void testQuery() throws Exception {
    DataInputStream dataInputStream = dataServiceClient.query( TEST_SQL_QUERY, MAX_ROWS );
    assertNotNull( dataInputStream );

    dataInputStream = dataServiceClient.query( TEST_DUMMY_SQL_QUERY, MAX_ROWS );
    assertNotNull( dataInputStream );

    when( builder.build() ).thenThrow( new KettleException() );
    dataServiceClient.query( TEST_SQL_QUERY, MAX_ROWS );

    verify( executor ).waitUntilFinished();
  }

  @Test
  public void testBuildExecutor() throws Exception {
    DataServiceExecutor.Builder builder = dataServiceClient.buildExecutor( sql );
    assertNotNull( builder );
  }

  @Test
  public void testWriteDummyRow() throws Exception {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream( byteArrayOutputStream );
    dataServiceClient.writeDummyRow( sql, dos );

    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( byteArrayOutputStream.toByteArray() );
    DataInputStream dataInputStream = new DataInputStream( byteArrayInputStream );

    assertEquals( DUAL_TABLE_NAME, dataInputStream.readUTF() );
  }

  @Test
  public void testFindDataService() throws Exception {
    when( metaStoreUtil.getDataService( sql.getServiceName(), repository, metaStore ) ).thenReturn( dataServiceMeta );
    DataServiceMeta dataServiceMeta1 = dataServiceClient.findDataService( sql );

    assertEquals( dataServiceMeta1, dataServiceMeta );

    when( sql.getServiceName() ).thenReturn( SERVICE_NAME );
    when( metaStoreUtil.getDataService( sql.getServiceName(), repository, metaStore ) ).thenThrow(
        new MetaStoreException() );

    try {
      dataServiceClient.findDataService( sql );
      fail();
    } catch ( Exception e ) {
      // Should get here
    }

    verify( metaStoreUtil ).getDataService( sql.getServiceName(), repository, metaStore );
  }

  @Test
  public void testGetServiceInformation() throws Exception {

    when( trans.getStepFields( dataServiceMeta.getStepname() ) ).thenReturn( rowMetaInterface );
    doNothing().when( trans ).activateParameters();

    List<DataServiceMeta> dataServices = new ArrayList<>();
    dataServices.add( dataServiceMeta );

    when( metaStoreUtil.getDataServices( any( Repository.class ), any( IMetaStore.class ), any( Function.class ) ) )
        .thenReturn( dataServices );

    List<ThinServiceInformation> serviceInformation = dataServiceClient.getServiceInformation();
    assertEquals( 1, serviceInformation.size() );

    when( trans.getStepFields( dataServiceMeta.getStepname() ) ).thenThrow( new KettleStepException() );

    try {
      dataServiceClient.getServiceInformation();
      fail();
    } catch ( Exception e ) {
      // Should get here
    }

    verify( trans, times( 2 ) ).activateParameters();
    verify( trans, times( 2 ) ).getStepFields( dataServiceMeta.getStepname() );
  }

  public void testGetRepository() {
    dataServiceClient.getRepository();
  }

  public void testSetRepository() {
    dataServiceClient.setRepository( repository );
  }

  public void testGetMetaStore() {
    dataServiceClient.getMetaStore();
  }

  public void testSetMetaStore() {
    dataServiceClient.setMetaStore( metaStore );
  }
}
