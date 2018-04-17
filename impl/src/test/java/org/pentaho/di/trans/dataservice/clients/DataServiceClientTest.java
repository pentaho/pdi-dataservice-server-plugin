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

package org.pentaho.di.trans.dataservice.clients;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
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
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.jdbc.ThinServiceInformation;
import org.pentaho.di.trans.dataservice.resolvers.DataServiceResolver;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
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
  private static final IDataServiceClientService.StreamingMode WINDOW_MODE
    = IDataServiceClientService.StreamingMode.ROW_BASED;
  private static final long WINDOW_SIZE = 10;
  private static final long WINDOW_EVERY = 50;
  private static final long WINDOW_MAX_SIZE = 1;

  private ImmutableMap<String, String> params = ImmutableMap.of();

  @Mock RowMetaInterface rowMetaInterface;
  @Mock DataServiceResolver dataServiceResolver;
  @Mock Query.Service queryServiceDelegate;
  @Mock LogChannelInterface log;
  @Mock LogChannelInterface dummyLog;
  @Mock Query mockQuery;

  @Before
  public void setUp() throws Exception {
    when( dataServiceResolver.getDataService( DATA_SERVICE_NAME ) ).thenReturn( dataService );
    when( dataServiceResolver.getDataService( STREAMING_DATA_SERVICE_NAME ) ).thenReturn( streamingDataService );
    when( dataServiceResolver.getDataServices( anyString(), any() ) ).thenReturn( ImmutableList.of( dataService ) );
    when( dataServiceResolver.getDataServices( any() ) ).thenReturn( ImmutableList.of( dataService ) );

    client = new DataServiceClient( queryServiceDelegate, dataServiceResolver, Executors.newCachedThreadPool() );
    client.setLogChannel( log );
  }

  @Test
  public void testLogChannel() {
    assertSame( log, client.getLogChannel() );
    client.setLogChannel( dummyLog );
    assertSame( dummyLog, client.getLogChannel() );
  }

  @Test
  public void testQuery() throws Exception {
    when( queryServiceDelegate.prepareQuery( TEST_SQL_QUERY, MAX_ROWS, ImmutableMap.of() ) )
      .thenReturn( (MockQuery) outputStream -> new DataOutputStream( outputStream ).writeUTF( QUERY_RESULT ) );

    try ( DataInputStream dataInputStream = client.query( TEST_SQL_QUERY, MAX_ROWS ) ) {
      assertThat( dataInputStream.readUTF(), equalTo( QUERY_RESULT ) );
      assertThat( dataInputStream.read(), equalTo( -1 ) ); // End of stream
    }

    verify( queryServiceDelegate ).prepareQuery( TEST_SQL_QUERY, MAX_ROWS, ImmutableMap.of() );
  }

  @Test
  public void testQueryParams() throws Exception {
    when( queryServiceDelegate.prepareQuery( TEST_SQL_QUERY, MAX_ROWS, params ) )
      .thenReturn( (MockQuery) outputStream -> new DataOutputStream( outputStream ).writeUTF( QUERY_RESULT ) );

    try ( DataInputStream dataInputStream = client.query( TEST_SQL_QUERY, MAX_ROWS, params ) ) {
      assertThat( dataInputStream.readUTF(), equalTo( QUERY_RESULT ) );
      assertThat( dataInputStream.read(), equalTo( -1 ) ); // End of stream
    }

    verify( queryServiceDelegate ).prepareQuery( TEST_SQL_QUERY, MAX_ROWS, params );
  }

  @Test
  public void testWindowQuery() throws Exception {
    when( queryServiceDelegate
      .prepareQuery( TEST_SQL_QUERY, WINDOW_MODE, WINDOW_SIZE, WINDOW_EVERY, WINDOW_MAX_SIZE, ImmutableMap.of() ) )
      .thenReturn( (MockQuery) outputStream -> new DataOutputStream( outputStream ).writeUTF( QUERY_RESULT ) );

    try ( DataInputStream dataInputStream = client.query( TEST_SQL_QUERY, WINDOW_MODE, WINDOW_SIZE,
      WINDOW_EVERY, WINDOW_MAX_SIZE ) ) {
      assertThat( dataInputStream.readUTF(), equalTo( QUERY_RESULT ) );
      assertThat( dataInputStream.read(), equalTo( -1 ) ); // End of stream
    }

    verify( queryServiceDelegate )
      .prepareQuery( TEST_SQL_QUERY, WINDOW_MODE, WINDOW_SIZE, WINDOW_EVERY, WINDOW_MAX_SIZE, ImmutableMap.of() );
  }

  @Test
  public void testWindowQueryParams() throws Exception {
    when( queryServiceDelegate
      .prepareQuery( TEST_SQL_QUERY, WINDOW_MODE, WINDOW_SIZE, WINDOW_EVERY, WINDOW_MAX_SIZE, params ) )
      .thenReturn( (MockQuery) outputStream -> new DataOutputStream( outputStream ).writeUTF( QUERY_RESULT ) );

    try ( DataInputStream dataInputStream = client.query( TEST_SQL_QUERY, WINDOW_MODE, WINDOW_SIZE,
      WINDOW_EVERY, WINDOW_MAX_SIZE, params ) ) {
      assertThat( dataInputStream.readUTF(), equalTo( QUERY_RESULT ) );
      assertThat( dataInputStream.read(), equalTo( -1 ) ); // End of stream
    }

    verify( queryServiceDelegate )
      .prepareQuery( TEST_SQL_QUERY, WINDOW_MODE, WINDOW_SIZE, WINDOW_EVERY, WINDOW_MAX_SIZE, params );
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
    doThrow( expected ).when( mockQuery ).writeTo( anyObject() );

    when( queryServiceDelegate.prepareQuery( TEST_SQL_QUERY, MAX_ROWS, ImmutableMap.of() ) )
      .thenReturn( mockQuery );

    try ( DataInputStream stream = client.query( TEST_SQL_QUERY, MAX_ROWS ) ) {
      assertThat( stream.read(), is( -1 ) );
    }
    verify( log ).logError( anyString(), eq( expected ) );
  }

  @Test
  public void testWindowQueryWriteFailure() throws Exception {
    IOException expected = new IOException();
    doThrow( expected ).when( mockQuery ).writeTo( anyObject() );

    when( queryServiceDelegate.prepareQuery( TEST_SQL_QUERY, WINDOW_MODE, WINDOW_SIZE, WINDOW_EVERY,
      WINDOW_MAX_SIZE, ImmutableMap.of() ) )
      .thenReturn( mockQuery );

    try ( DataInputStream stream = client.query( TEST_SQL_QUERY, WINDOW_MODE, WINDOW_SIZE, WINDOW_EVERY,
      WINDOW_MAX_SIZE ) ) {
      assertThat( stream.read(), is( -1 ) );
    }
    verify( log ).logError( anyString(), eq( expected ) );
  }

  @Test
  public void testWindowQueryFailure() throws Exception {
    KettleException expected = new KettleException();
    when( queryServiceDelegate.prepareQuery( eq( TEST_SQL_QUERY ), eq( WINDOW_MODE ), eq( WINDOW_SIZE ),
      eq( WINDOW_EVERY ), eq( WINDOW_MAX_SIZE ), any() ) ).thenThrow( expected );

    try ( DataInputStream ignored = client.query( TEST_SQL_QUERY,  WINDOW_MODE, WINDOW_SIZE, WINDOW_EVERY,
      WINDOW_MAX_SIZE ) ) {
      fail( "Query should have failed" );
    } catch ( SQLException e ) {
      assertThat( e.getCause(), is( expected ) );
    }
  }

  @Test( expected = SQLException.class )
  public void testUnableToResolveQueryException() throws Exception {
    when( queryServiceDelegate.prepareQuery( TEST_SQL_QUERY, MAX_ROWS, ImmutableMap.of() ) )
      .thenReturn( null );

    client.query( TEST_SQL_QUERY, MAX_ROWS );
  }

  @Test( expected = SQLException.class )
  public void testUnableToResolveWindowQueryException() throws Exception {
    when( queryServiceDelegate.prepareQuery( TEST_SQL_QUERY, WINDOW_MODE, WINDOW_SIZE, WINDOW_EVERY,
      WINDOW_MAX_SIZE, ImmutableMap.of() ) )
      .thenReturn( null );

    client.query( TEST_SQL_QUERY, WINDOW_MODE, WINDOW_SIZE, WINDOW_EVERY, WINDOW_MAX_SIZE );
  }

  @Test
  public void testInputStreamClosed() throws Exception {
    CountDownLatch startQuery = new CountDownLatch( 1 );
    SettableFuture<Boolean> queryFinished = SettableFuture.create();

    // Create a query that blocks on the countdown latch
    when( queryServiceDelegate.prepareQuery( TEST_SQL_QUERY, MAX_ROWS, ImmutableMap.of() ) )
      .thenReturn( (MockQuery) outputStream -> {
        Random random = new Random();
        try {
          outputStream.write( random.nextInt() );
          startQuery.await();
          outputStream.write( random.nextInt() );
          queryFinished.set( true );
        } catch ( Exception e ) {
          queryFinished.setException( e );
        }
      } );

    // Start query and close immediately
    client.query( TEST_SQL_QUERY, MAX_ROWS ).close();
    // Unleash query stream
    startQuery.countDown();

    // Assert no errors thrown or logged
    assertThat( Futures.get( queryFinished, IOException.class ), is( true ) );
    verify( log, never() ).logError( anyString(), any( Throwable.class ) );
  }

  @Test
  public void testWindowInputStreamClosed() throws Exception {
    CountDownLatch startQuery = new CountDownLatch( 1 );
    SettableFuture<Boolean> queryFinished = SettableFuture.create();

    // Create a query that blocks on the countdown latch
    when( queryServiceDelegate.prepareQuery( TEST_SQL_QUERY, WINDOW_MODE, WINDOW_SIZE, WINDOW_EVERY,
      WINDOW_MAX_SIZE, ImmutableMap.of() ) )
      .thenReturn( (MockQuery) outputStream -> {
        Random random = new Random();
        try {
          outputStream.write( random.nextInt() );
          startQuery.await();
          outputStream.write( random.nextInt() );
          queryFinished.set( true );
        } catch ( Exception e ) {
          queryFinished.setException( e );
        }
      } );

    // Start query and close immediately
    client.query( TEST_SQL_QUERY, WINDOW_MODE, WINDOW_SIZE, WINDOW_EVERY, WINDOW_MAX_SIZE ).close();
    // Unleash query stream
    startQuery.countDown();

    // Assert no errors thrown or logged
    assertThat( Futures.get( queryFinished, IOException.class ), is( true ) );
    verify( log, never() ).logError( anyString(), any( Throwable.class ) );
  }

  @Test
  public void testGetServiceInformation() throws Exception {
    when( transMeta.getStepFields( dataService.getStepname() ) ).thenReturn( rowMetaInterface );

    assertThat( client.getServiceInformation(), contains( allOf(
      hasProperty( "name", equalTo( DATA_SERVICE_NAME ) ),
      hasProperty( "serviceFields", equalTo( rowMetaInterface ) ),
      hasProperty( "streaming", equalTo( false ) )
    ) ) );
    verify( transMeta ).activateParameters();

    when( transMeta.getStepFields( DATA_SERVICE_STEP ) ).thenThrow( new KettleStepException() );
    assertThat( client.getServiceInformation(), is( empty() ) );
  }

  @Test
  public void testWindowGetServiceInformation() throws Exception {
    when( streamingTransMeta.getStepFields( streamingDataService.getStepname() ) ).thenReturn( rowMetaInterface );
    when( dataServiceResolver.getDataServices( anyString(), any() ) ).thenReturn( ImmutableList.of( streamingDataService ) );
    when( dataServiceResolver.getDataServices( any() ) ).thenReturn( ImmutableList.of( streamingDataService ) );

    assertThat( client.getServiceInformation(), contains( allOf(
      hasProperty( "name", equalTo( STREAMING_DATA_SERVICE_NAME ) ),
      hasProperty( "serviceFields", equalTo( rowMetaInterface ) ),
      hasProperty( "streaming", equalTo( true ) )
    ) ) );
    verify( streamingTransMeta ).activateParameters();

    when( streamingTransMeta.getStepFields( DATA_SERVICE_STEP ) ).thenThrow( new KettleStepException() );
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

  @Test
  public void testServiceNameResolution() throws Exception {
    when( dataServiceResolver.getDataServiceNames() ).thenReturn( ImmutableList.of( "A", "B" ) );
    when( dataServiceResolver.getDataServiceNames( "A" ) ).thenReturn( ImmutableList.of( "A" ) );

    assertThat( client.getServiceNames(), contains( "A", "B" ) );
    assertThat( client.getServiceNames( "A" ), contains( "A" ) );
  }

  private interface MockQuery extends Query {
    default List<Trans> getTransList() {
      return ImmutableList.of();
    }
  }
}
