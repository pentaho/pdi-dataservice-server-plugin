/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.BaseTest;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.jdbc.ThinServiceInformation;
import org.pentaho.di.trans.dataservice.resolvers.DataServiceResolver;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @author bmorrise
 */
@RunWith( MockitoJUnitRunner.class )
public class DataServiceClientTest extends BaseTest {

  private static final String TEST_SQL_QUERY = "SELECT * FROM " + DATA_SERVICE_NAME;
  private static final String QUERY_RESULT = "...query result...";
  private static final int MAX_ROWS = 100;

  @Mock DataServiceExecutor executor;
  @Mock RowMetaInterface rowMetaInterface;
  @Mock MetastoreLocator metastoreLocator;
  @Mock DataServiceResolver dataServiceResolver;
  @Mock Query.Service queryServiceDelegate;
  @Mock Query query;
  @Mock LogChannelInterface log;

  @Before
  public void setUp() throws Exception {
    when( dataServiceResolver.getDataService( DATA_SERVICE_NAME ) ).thenReturn( dataService );
    when( dataServiceResolver.getDataServices( anyString(), any() ) ).thenReturn( ImmutableList.of( dataService ) );
    when( dataServiceResolver.getDataServices( any() ) ).thenReturn( ImmutableList.of( dataService ) );

    when( queryServiceDelegate.prepareQuery( TEST_SQL_QUERY, MAX_ROWS, ImmutableMap.of() ) ).thenReturn( query );
    when( query.getTransList() ).thenReturn( ImmutableList.of() );
    
    client = new DataServiceClient( queryServiceDelegate, dataServiceResolver, Executors.newCachedThreadPool() );
    client.setLogChannel( log );
  }

  @Test
  public void testQuery() throws Exception {
    doAnswer( invocation -> {
      new DataOutputStream( (OutputStream) invocation.getArguments()[ 0 ] ).writeUTF( QUERY_RESULT );
      return null;
    } ).when( query ).writeTo( any( OutputStream.class ) );

    try ( DataInputStream dataInputStream = client.query( TEST_SQL_QUERY, MAX_ROWS ) ) {
      assertThat( dataInputStream.readUTF(), equalTo( QUERY_RESULT ) );
      assertThat( dataInputStream.read(), equalTo( -1 ) ); // End of stream
    }

    verify( queryServiceDelegate ).prepareQuery( TEST_SQL_QUERY, MAX_ROWS, ImmutableMap.of() );
  }

  @Test
  public void testQueryFailure() throws Exception {
    KettleException expected = new KettleException();
    when( queryServiceDelegate.prepareQuery( eq( TEST_SQL_QUERY ), eq( MAX_ROWS ), any() ) ).thenThrow( expected );

    try ( DataInputStream ignored = client.query( TEST_SQL_QUERY, MAX_ROWS ) ) {
      fail( "Query should have failed" );
    } catch ( SQLException e ) {
      assertThat( e.getCause(), is( expected ) );
    }
  }

  @Test
  public void testWriteFailure() throws Exception {
    IOException expected = new IOException();
    doThrow( expected ).when( query ).writeTo( any() );

    try ( DataInputStream stream = client.query( TEST_SQL_QUERY, MAX_ROWS ) ) {
      assertThat( stream.read(), is( -1 ) );
    }
    verify( log ).logError( anyString(), eq( expected ) );
  }

  @Test
  public void testGetServiceInformation() throws Exception {
    when( transMeta.getStepFields( dataService.getStepname() ) ).thenReturn( rowMetaInterface );

    assertThat( client.getServiceInformation(), contains( allOf(
      hasProperty( "name", equalTo( DATA_SERVICE_NAME ) ),
      hasProperty( "serviceFields", equalTo( rowMetaInterface ) )
    ) ) );
    verify( transMeta ).activateParameters();

    when( transMeta.getStepFields( DATA_SERVICE_STEP ) ).thenThrow( new KettleStepException() );
    assertThat( client.getServiceInformation(), is( empty() ) );
  }

  @Test
  public void testGetServiceInformationByName() throws Exception {
    when( transMeta.getStepFields( dataService.getStepname() ) ).thenReturn( rowMetaInterface );

    ThinServiceInformation serviceInformation = client.getServiceInformation( DATA_SERVICE_NAME );
    verify( transMeta ).activateParameters();

    assertThat( serviceInformation.getName(), equalTo( DATA_SERVICE_NAME ) );
    assertThat( serviceInformation.getServiceFields(), equalTo( rowMetaInterface ) );
  }
}
