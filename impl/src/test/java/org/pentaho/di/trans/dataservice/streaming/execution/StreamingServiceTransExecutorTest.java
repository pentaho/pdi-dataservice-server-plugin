/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.streaming.execution;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.utils.DataServiceConstants;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepInterface;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.pentaho.di.core.util.Assert.assertNull;

/**
 * {@link StreamExecutionListener} test class
 */
@RunWith( MockitoJUnitRunner.class )
public class StreamingServiceTransExecutorTest {
  private StreamingServiceTransExecutor serviceExecutor;
  private String MOCK_ID = "Mock Id";
  private String MOCK_SERVICE_STEP_NAME = "Mock Step Name";
  private String MOCK_QUERY = "Mock Query";
  private String MOCK_ROW_META_STRING = "Mock Row Meta String";
  private int MOCK_ROW_LIMIT = 10;
  private long MOCK_TIME_LIMIT = 1000;
  private IDataServiceClientService.StreamingMode MOCK_WINDOW_MODE_ROW_BASED
    = IDataServiceClientService.StreamingMode.ROW_BASED;
  private IDataServiceClientService.StreamingMode MOCK_WINDOW_MODE_TIME_BASED
    = IDataServiceClientService.StreamingMode.TIME_BASED;

  @Mock Trans serviceTrans;
  @Mock TransMeta serviceTransMeta;
  @Mock StepInterface serviceStep;
  @Mock RowMetaInterface rowMetaInterface;
  @Mock LogChannelInterface log;

  @Before
  public void setup() throws Exception {
    serviceExecutor = new StreamingServiceTransExecutor( MOCK_ID, serviceTrans, MOCK_SERVICE_STEP_NAME,
      MOCK_ROW_LIMIT, MOCK_TIME_LIMIT );
    when( serviceTrans.findRunThread( MOCK_SERVICE_STEP_NAME ) ).thenReturn( serviceStep );
    when( serviceTrans.getLogChannel( ) ).thenReturn( log );
    when( serviceTrans.getTransMeta( ) ).thenReturn( serviceTransMeta );
    when( serviceTransMeta.getStepFields( MOCK_SERVICE_STEP_NAME ) ).thenReturn( rowMetaInterface );
    when( rowMetaInterface.getString( any( Object[].class ) ) ).thenReturn( MOCK_ROW_META_STRING );
    when( log.isRowLevel( ) ).thenReturn( true );
  }

  @Test
  public void testGetServiceTrans() {
    assertSame( serviceTrans, serviceExecutor.getServiceTrans() );
  }

  @Test
  public void testGetWindowMaxRowLimit() {
    assertEquals( MOCK_ROW_LIMIT, serviceExecutor.getWindowMaxRowLimit() );
  }

  @Test
  public void testGetWindowMaxTimeLimit() {
    assertEquals( MOCK_TIME_LIMIT, serviceExecutor.getWindowMaxTimeLimit() );
  }

  @Test
  public void testGetId() {
    assertSame( MOCK_ID, serviceExecutor.getId() );
  }

  @Test ( expected = RuntimeException.class )
  public void testServiceStepNotFound() {
    when( serviceTrans.findRunThread( MOCK_SERVICE_STEP_NAME ) ).thenReturn( null );
    serviceExecutor.getBuffer( MOCK_QUERY, MOCK_WINDOW_MODE_ROW_BASED, 1, 0, 0 );
  }

  @Test ( expected = RuntimeException.class )
  public void testServiceStartThreadException() throws KettleException {
    doThrow( new KettleException() ).when( serviceTrans ).startThreads();
    serviceExecutor.getBuffer( MOCK_QUERY, MOCK_WINDOW_MODE_ROW_BASED, 1, 0, 0 );
  }

  @Test
  public void testGetBufferWindowNullInvalidValues() throws Exception {
    StreamExecutionListener buffer = serviceExecutor.getBuffer( MOCK_QUERY, MOCK_WINDOW_MODE_ROW_BASED, 0, 0, 0 );
    assertNull( buffer );

    buffer = serviceExecutor.getBuffer( MOCK_QUERY, MOCK_WINDOW_MODE_ROW_BASED, -1, 0, 0 );
    assertNull( buffer );

    buffer = serviceExecutor.getBuffer( MOCK_QUERY, MOCK_WINDOW_MODE_ROW_BASED, 0, -1, 0 );
    assertNull( buffer );

    buffer = serviceExecutor.getBuffer( MOCK_QUERY, MOCK_WINDOW_MODE_ROW_BASED, -1, -1, -1 );
    assertNull( buffer );
  }

  @Test
  public void testGetBufferWindowRowSize() throws Exception {
    serviceExecutor.getBuffer( MOCK_QUERY, MOCK_WINDOW_MODE_ROW_BASED, 1, 0, 10 );
    testBufferAux();
  }

  @Test
  public void testGetBufferWindowRowSizeRate() throws Exception {
    serviceExecutor.getBuffer( MOCK_QUERY, MOCK_WINDOW_MODE_ROW_BASED, 1, 1, 10 );
    testBufferAux();
  }

  @Test
  public void testGetBufferWindowSizeTime() throws Exception {
    serviceExecutor.getBuffer( MOCK_QUERY, MOCK_WINDOW_MODE_TIME_BASED, 1000, 0, 10000 );
    testBufferAux();
  }

  @Test
  public void testGetBufferWindowSizeTimeRate() throws Exception {
    serviceExecutor.getBuffer( MOCK_QUERY, MOCK_WINDOW_MODE_TIME_BASED, 1000, 1000, 10000 );
    testBufferAux();
  }

  @Test
  public void testGetBufferTime2Calls() throws Exception {
    serviceExecutor.getBuffer( MOCK_QUERY, MOCK_WINDOW_MODE_TIME_BASED, 1000, 0, 10000 );
    serviceExecutor.getBuffer( MOCK_QUERY, MOCK_WINDOW_MODE_TIME_BASED, 1000, 0, 10000 );
    testBufferAux();
  }

  @Test
  public void testGetBufferRows2Calls() throws Exception {
    serviceExecutor.getBuffer( MOCK_QUERY, MOCK_WINDOW_MODE_ROW_BASED, 1, 0, 10 );
    serviceExecutor.getBuffer( MOCK_QUERY, MOCK_WINDOW_MODE_ROW_BASED, 1, 0, 10 );
    testBufferAux();
  }

  @Test
  public void testGetBufferTimeRate2Calls() throws Exception {
    serviceExecutor.getBuffer( MOCK_QUERY, MOCK_WINDOW_MODE_TIME_BASED, 1000, 1000, 10000 );
    serviceExecutor.getBuffer( MOCK_QUERY, MOCK_WINDOW_MODE_TIME_BASED, 1000, 1000, 10000 );
    testBufferAux();
  }

  @Test
  public void testGetBufferRowsRate2Calls() throws Exception {
    serviceExecutor.getBuffer( MOCK_QUERY, MOCK_WINDOW_MODE_ROW_BASED, 1, 1, 10 );
    serviceExecutor.getBuffer( MOCK_QUERY, MOCK_WINDOW_MODE_ROW_BASED, 1, 1, 10 );
    testBufferAux();
  }

  private void testBufferAux() throws Exception {
    ArgumentCaptor<RowListener> listenerArgumentCaptor = ArgumentCaptor.forClass( RowListener.class );

    verify( serviceStep ).addRowListener( listenerArgumentCaptor.capture() );
    verify( serviceTrans, times( 1 ) ).startThreads( );
    verify( serviceTrans, times( 1 ) ).findRunThread( MOCK_SERVICE_STEP_NAME );
    verify( serviceStep, times( 1 ) ).addRowListener( any( RowAdapter.class ) );

    // Verify linkage
    RowListener serviceRowListener = listenerArgumentCaptor.getValue();
    assertNotNull( serviceRowListener );

    RowMetaInterface rowMeta = serviceTrans.getTransMeta().getStepFields( MOCK_SERVICE_STEP_NAME );
    Object[] data = new Object[] { 0 };
    serviceRowListener.rowWrittenEvent( rowMeta, data );
    verify( log ).logRowlevel( DataServiceConstants.PASSING_ALONG_ROW + rowMeta.getString( data ) );
    verify( serviceTrans, never( ) ).stopAll( );
    verify( serviceTrans, never( ) ).waitUntilFinished( );
    verify( log, never( ) ).logDebug( DataServiceConstants.STREAMING_TRANSFORMATION_STOPPED );
  }

  @Test
  public void testGetBufferCleanup() throws Exception {
    serviceExecutor.getBuffer( MOCK_QUERY, MOCK_WINDOW_MODE_ROW_BASED, 1, 0, 0 );

    ArgumentCaptor<RowListener> listenerArgumentCaptor = ArgumentCaptor.forClass( RowListener.class );

    verify( serviceStep ).addRowListener( listenerArgumentCaptor.capture() );
    verify( serviceTrans ).startThreads( );
    verify( serviceTrans ).findRunThread( MOCK_SERVICE_STEP_NAME );
    verify( serviceStep ).addRowListener( any( RowAdapter.class ) );

    // Verify linkage
    RowListener serviceRowListener = listenerArgumentCaptor.getValue();
    assertNotNull( serviceRowListener );

    RowMetaInterface rowMeta = serviceTrans.getTransMeta().getStepFields( MOCK_SERVICE_STEP_NAME );
    Object[] data = new Object[] { 0 };

    serviceExecutor.clearCache();

    serviceRowListener.rowWrittenEvent( rowMeta, data );
    verify( log ).logRowlevel( DataServiceConstants.PASSING_ALONG_ROW + rowMeta.getString( data ) );
    verify( serviceTrans ).stopAll( );
    verify( serviceTrans ).waitUntilFinished( );
    verify( log ).logDetailed( DataServiceConstants.STREAMING_TRANSFORMATION_STOPPED );
  }
}
