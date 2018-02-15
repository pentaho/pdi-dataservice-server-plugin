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

import io.reactivex.Observable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.streaming.StreamList;
import org.pentaho.di.trans.dataservice.utils.DataServiceConstants;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepInterface;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link StreamingGeneratedTransExecution} test class
 */
@RunWith( MockitoJUnitRunner.class )
public class StreamingGeneratedTransExecutionTest {
  private StreamingGeneratedTransExecution gentransExecutor;

  private String MOCK_RESULT_STEP_NAME = "Mock Result Step Name";
  private String MOCK_INJECTOR_STEP_NAME = "Mock Injector Step Name";
  private String MOCK_QUERY = "Mock Query";
  private int MOCK_WINDOW_SIZE = 1;
  private long MOCK_WINDOW_MILLIS = 0;
  private List<RowMetaAndData> rowIterator;
  private StreamList<RowMetaAndData> streamList;
  private Observable<List<RowMetaAndData>> mockObservable;

  @Mock StreamingServiceTransExecutor serviceExecutor;
  @Mock Trans genTrans;
  @Mock RowListener resultRowListener;
  @Mock StreamExecutionListener streamExecutionListener;
  @Mock RowMetaAndData rowMetaAndData;
  @Mock LogChannelInterface log;
  @Mock StepInterface resultStep;
  @Mock RowProducer rowProducer;
  @Mock RowMetaAndData mockRowMetaAndData;

  @Before
  public void setup() throws Exception {
    rowIterator = new ArrayList();
    rowIterator.add( rowMetaAndData );
    streamList = new StreamList();
    mockObservable = streamList.getStream().buffer( 1 );

    when( streamExecutionListener.getBuffer( ) ).thenReturn( mockObservable );
    when( serviceExecutor.getBuffer( MOCK_QUERY, MOCK_WINDOW_SIZE, MOCK_WINDOW_MILLIS ) )
      .thenReturn( streamExecutionListener );
    when( log.isRowLevel() ).thenReturn( true );
    when( genTrans.getLogChannel() ).thenReturn( log );
    when( genTrans.findRunThread( MOCK_RESULT_STEP_NAME ) ).thenReturn( resultStep );
    when( genTrans.addRowProducer( MOCK_INJECTOR_STEP_NAME, 0 ) ).thenReturn( rowProducer );

    gentransExecutor = new StreamingGeneratedTransExecution( serviceExecutor, genTrans, resultRowListener,
      MOCK_INJECTOR_STEP_NAME, MOCK_RESULT_STEP_NAME, MOCK_QUERY, MOCK_WINDOW_SIZE, MOCK_WINDOW_MILLIS );
  }

  @Test
  public void testRunCachedWindow() throws Exception {
    when( streamExecutionListener.hasCachedWindow() ).thenReturn( true );
    when( streamExecutionListener.getCachedWindow() ).thenReturn( rowIterator );

    gentransExecutor.run();

    verifyExecution( 1 );
  }

  @Test( expected = RuntimeException.class )
  public void testRunException() {
    when( streamExecutionListener.hasCachedWindow() ).thenThrow( new KettleException( ) );

    gentransExecutor.run();
  }

  @Test
  public void testPutRowLog() {
    when( rowProducer.putRowWait( any( RowMetaInterface.class ), any( Object[].class ), anyLong(),
      any( TimeUnit.class ) ) ).thenReturn( false, true );
    when( genTrans.isRunning() ).thenReturn( true );
    when( streamExecutionListener.hasCachedWindow() ).thenReturn( true );
    when( streamExecutionListener.getCachedWindow() ).thenReturn( rowIterator );

    gentransExecutor.run();
    verify( log ).logRowlevel( DataServiceConstants.ROW_BUFFER_IS_FULL_TRYING_AGAIN );
  }

  @Test
  public void testRunNoCachedWindow() throws Exception {
    when( streamExecutionListener.hasCachedWindow() ).thenReturn( false );

    gentransExecutor.run();

    verify( streamExecutionListener, times( 0 ) ).getCachedWindow();
    verifyExecution( 0 );

    streamList.add( mockRowMetaAndData );

    verifyExecution( 1 );
  }

  private void verifyExecution( int numExecs ) throws Exception {
    verify( genTrans, times( numExecs ) ).cleanup( );
    verify( genTrans, times( numExecs ) ).prepareExecution( null );
    verify( genTrans, times( numExecs ) ).startThreads( );
    verify( genTrans, times( numExecs ) ).findRunThread( MOCK_RESULT_STEP_NAME );
    verify( resultStep, times( numExecs ) ).cleanup( );
    verify( resultStep, times( numExecs ) ).addRowListener( resultRowListener );
    verify( rowProducer, times( numExecs ) ).finished( );
    verify( genTrans, times( numExecs ) ).waitUntilFinished( );
    verify( genTrans, times( numExecs ) ).stopAll( );
  }
}
