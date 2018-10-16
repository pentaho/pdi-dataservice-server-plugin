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

import io.reactivex.Observer;
import io.reactivex.functions.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.streaming.StreamServiceKey;
import org.pentaho.di.trans.dataservice.utils.DataServiceConstants;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepInterface;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link StreamingGeneratedTransExecution} test class
 */
@RunWith( MockitoJUnitRunner.class )
public class StreamingGeneratedTransExecutionTest {
  private StreamingGeneratedTransExecution genTransExecutor;

  private String MOCK_RESULT_STEP_NAME = "Mock Result Step Name";
  private String MOCK_INJECTOR_STEP_NAME = "Mock Injector Step Name";
  private String MOCK_QUERY = "Mock Query";
  private String MOCK_KEY = "Dataservice key";
  private IDataServiceClientService.StreamingMode MOCK_WINDOW_MODE_ROW_BASED
    = IDataServiceClientService.StreamingMode.ROW_BASED;
  private long MOCK_WINDOW_SIZE = 1;
  private long MOCK_WINDOW_EVERY = 1;
  private long MOCK_WINDOW_MAX_SIZE = 10;
  private List<RowMetaAndData> rowIterator;
  private StreamingServiceTransExecutor serviceExecutor;
  private StreamServiceKey streamServiceKey;

  @Mock Trans genTrans;
  @Mock Trans serviceTrans;
  @Mock DataServiceContext context;
  @Mock Observer<List<RowMetaAndData>> consumer;
  Boolean pollingMode = Boolean.FALSE;
  @Mock StreamExecutionListener streamExecutionListener;
  @Mock RowMetaAndData rowMetaAndData;
  @Mock LogChannelInterface log;
  StepInterface resultStep = mock( BaseStep.class );
  @Mock RowProducer rowProducer;
  @Mock Consumer<List<RowMetaAndData>> windowConsumer;

  @Before
  public void setup() throws Exception {
    rowIterator = new ArrayList<>();
    rowIterator.add( rowMetaAndData );

    when( serviceTrans.findRunThread( MOCK_RESULT_STEP_NAME ) ).thenReturn( resultStep );

    streamServiceKey = StreamServiceKey.create( MOCK_KEY, Collections.emptyMap(), Collections.emptyList() );
    serviceExecutor = spy( new StreamingServiceTransExecutor( streamServiceKey, serviceTrans, MOCK_RESULT_STEP_NAME, 10000, 1000, context ) );

    doReturn( streamExecutionListener ).when( serviceExecutor ).getBuffer( MOCK_QUERY, windowConsumer, MOCK_WINDOW_MODE_ROW_BASED, MOCK_WINDOW_SIZE,
      MOCK_WINDOW_EVERY, MOCK_WINDOW_MAX_SIZE );

    when( log.isRowLevel() ).thenReturn( true );
    when( genTrans.getLogChannel() ).thenReturn( log );
    when( genTrans.findRunThread( MOCK_RESULT_STEP_NAME ) ).thenReturn( resultStep );
    when( genTrans.addRowProducer( MOCK_INJECTOR_STEP_NAME, 0 ) ).thenReturn( rowProducer );
    when( genTrans.isRunning() ).thenReturn( true );

    genTransExecutor = spy( new StreamingGeneratedTransExecution( serviceExecutor, genTrans, consumer, pollingMode,
      MOCK_INJECTOR_STEP_NAME, MOCK_RESULT_STEP_NAME, MOCK_QUERY, MOCK_WINDOW_MODE_ROW_BASED, MOCK_WINDOW_SIZE,
      MOCK_WINDOW_EVERY, MOCK_WINDOW_MAX_SIZE, "" ) );
    when ( genTransExecutor.getWindowConsumer() ).thenReturn( windowConsumer );
  }

  @Test
  public void testRunCachedWindow() throws Exception {
    when( streamExecutionListener.getCachePreWindow() ).thenReturn( rowIterator );
    when( genTrans.isRunning() ).thenReturn( true );

    genTransExecutor = spy( new StreamingGeneratedTransExecution( serviceExecutor, genTrans, consumer, pollingMode,
      MOCK_INJECTOR_STEP_NAME, MOCK_RESULT_STEP_NAME, MOCK_QUERY, MOCK_WINDOW_MODE_ROW_BASED, 10000,
      1, 10000, "" ) );

    genTransExecutor.run();

    verifyExecution( 0 );
  }

  @Test( expected = RuntimeException.class )
  public void testThrowExceptionWhenRunningTransExecutor() throws Exception {
    doReturn( null ).when( serviceExecutor ).getBuffer( MOCK_QUERY, windowConsumer, MOCK_WINDOW_MODE_ROW_BASED, MOCK_WINDOW_SIZE,
      MOCK_WINDOW_EVERY, MOCK_WINDOW_MAX_SIZE );
    when( genTrans.isRunning() ).thenReturn( true );
    doThrow( new KettleStepException( "This is expected" ) ).when( genTransExecutor ).runGenTrans( Collections.emptyList() );

    genTransExecutor.run();
  }

  @Test
  public void testRunEmptyStream() throws Exception {
    doReturn( null ).when( serviceExecutor ).getBuffer( MOCK_QUERY, windowConsumer, MOCK_WINDOW_MODE_ROW_BASED, MOCK_WINDOW_SIZE,
      MOCK_WINDOW_EVERY, MOCK_WINDOW_MAX_SIZE );
    when( genTrans.isRunning() ).thenReturn( true );

    genTransExecutor.run();

    verifyExecution( 1 );
  }

  @Test
  public void testRunException() throws KettleException {
    //since the processing is now aync, the exception should not interrupt the test (but should be thrown anyway: visible on test log)
    doThrow( new KettleException("This exception is expected!!!") ).when( genTrans ).startThreads();

    genTransExecutor.run();
    genTransExecutor.getGeneratedDataObservable().onNext( rowIterator );

    verify( genTrans, times( 1 ) ).startThreads( );
  }

  @Test
  public void testPutRowLog() {
    when( rowProducer.putRowWait( any( RowMetaInterface.class ), any( Object[].class ), anyLong(),
      any( TimeUnit.class ) ) ).thenReturn( false, true );

    genTransExecutor.run();
    genTransExecutor.getGeneratedDataObservable().onNext( rowIterator );

    genTrans.waitUntilFinished();

    verify( log ).logRowlevel( DataServiceConstants.ROW_BUFFER_IS_FULL_TRYING_AGAIN );
  }

  @Test
  public void testInjectRows(){
    List<RowMetaAndData> rowIterator = new ArrayList<>();
    LogChannelInterface log = mock( LogChannelInterface.class );
    RowProducer rowProducer = mock( RowProducer.class );

    RowMeta rowMeta = new RowMeta( );
    Object data = "data";
    Object[] dataArray = new Object[] { data };

    RowMetaAndData rowMetaAndData = new RowMetaAndData( rowMeta, data );
    rowIterator.add( rowMetaAndData );

    doReturn( true ).when( rowProducer ).putRowWait( rowMeta, dataArray, 1, TimeUnit.SECONDS );
    doCallRealMethod().when( genTransExecutor ).injectRows( rowIterator, log, rowProducer );

    genTransExecutor.injectRows( rowIterator, log, rowProducer );

    verify( rowProducer, times( 1 ) ).putRowWait( rowMeta, dataArray, 1, TimeUnit.SECONDS );
  }

  @Test
  public void testInjectNoRows(){
    List<RowMetaAndData> rowIterator = new ArrayList<>();
    LogChannelInterface log = mock( LogChannelInterface.class );
    RowProducer rowProducer = mock( RowProducer.class );

    RowMeta rowMeta = new RowMeta( );
    Object[] dataArray = new Object[0];

    doReturn( true ).when( rowProducer ).putRowWait( rowMeta, dataArray, 1, TimeUnit.SECONDS );
    doCallRealMethod().when( genTransExecutor ).injectRows( rowIterator, log, rowProducer );

    genTransExecutor.injectRows( rowIterator, log, rowProducer );

    verify( rowProducer, times( 0 ) ).putRowWait( rowMeta, dataArray, 1, TimeUnit.SECONDS );
  }

  @Test
  public void testGetRowListener() throws Exception {
    List<RowMetaAndData> tempList = new ArrayList<>( );
    RowAdapter rowAdapter = genTransExecutor.getRowWrittenListener( tempList );

    RowMeta rowMeta = new RowMeta( );
    Object data = "data";
    Object[] dataArray = new Object[] { data };

    rowAdapter.rowWrittenEvent( rowMeta, dataArray );

    assertEquals( dataArray.length, tempList.size() );
    assertNotNull( tempList.get(0).getData() );
    assertEquals( dataArray.length, tempList.get(0).getData().length );
    assertSame( data, tempList.get(0).getData()[0] );
  }

  @Test
  public void testWaitForGeneratedTransToFinnish(){
    doThrow( new RuntimeException( new InterruptedException( ) ) ).when( genTrans ).waitUntilFinished();
    genTransExecutor.waitForGeneratedTransToFinnish();
  }

  @Test ( expected = RuntimeException.class )
  public void testWaitForGeneratedTransToFinnishCauseNotInterruptedException(){
    doThrow( new RuntimeException( new NullPointerException( ) ) ).when( genTrans ).waitUntilFinished();
    genTransExecutor.waitForGeneratedTransToFinnish();
    fail( "When the thrown exception is not a RuntimeException with a InterruptedException cause, it should rethrow a RuntimeException" );
  }

  @Test ( expected = RuntimeException.class )
  public void testWaitForGeneratedTransToFinnishNullPointerExceptionCatch(){
    doThrow( new NullPointerException( ) ).when( genTrans ).waitUntilFinished();
    genTransExecutor.waitForGeneratedTransToFinnish();
    fail( "When the thrown exception is not a RuntimeException it should rethrow a RuntimeException" );
  }

  private void verifyExecution( int numExecs ) throws Exception {
    verify( genTrans, times( numExecs ) ).cleanup( );
    verify( genTrans, times( numExecs ) ).prepareExecution( null );
    verify( genTrans, times( numExecs ) ).startThreads( );
    verify( genTrans, times( numExecs ) ).findRunThread( MOCK_RESULT_STEP_NAME );
    verify( resultStep, times( numExecs ) ).cleanup( );
    verify( resultStep, times( 1 ) ).addRowListener( any( RowListener.class ) );
    verify( rowProducer, times( numExecs ) ).finished( );
    verify( genTrans, times( numExecs ) ).waitUntilFinished( );
    verify( genTrans, times( numExecs ) ).stopAll( );
  }
}
