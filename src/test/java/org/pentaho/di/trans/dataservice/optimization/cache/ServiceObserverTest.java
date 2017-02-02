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

package org.pentaho.di.trans.dataservice.optimization.cache;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepAdapter;
import org.pentaho.di.trans.step.StepInterface;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class ServiceObserverTest {
  @Mock( answer = Answers.RETURNS_DEEP_STUBS ) DataServiceExecutor executor;
  @Mock StepInterface stepInterface;
  @Mock RowMetaInterface rowMeta;
  @Mock KettleValueException exception;

  @Captor ArgumentCaptor<RowAdapter> rowAdapterCaptor;
  @Captor ArgumentCaptor<StepAdapter> stepAdapterCaptor;
  @InjectMocks ServiceObserver observer;
  Object[] row = new Object[0];
  final String STEPNAME = "My Stepname";

  @Before public void before() {
    when( executor.getService().getStepname() ).thenReturn( STEPNAME );
    when( executor.getServiceTrans().findRunThread( STEPNAME ) ).thenReturn( stepInterface );
  }

  @Test
  public void verifyCachedRowIsCloned() throws Exception {
    when( stepInterface.isStopped() ).thenReturn( false );
    observer.run();
    verify( stepInterface ).addRowListener( rowAdapterCaptor.capture() );
    verify( stepInterface ).addStepListener( stepAdapterCaptor.capture() );

    RowAdapter rowAdapter = rowAdapterCaptor.getValue();
    StepAdapter stepAdapter = stepAdapterCaptor.getValue();
    Object[] clonedRow = new Object[0];
    when( rowMeta.cloneRow( row ) ).thenReturn( clonedRow );
    rowAdapter.rowWrittenEvent( rowMeta, row );
    verify( rowMeta ).cloneRow( row );
    stepAdapter.stepFinished( null, null, stepInterface );

    CachedService cachedService = observer.get();
    assertThat( cachedService.getRowMetaAndData().get( 0 ).getData(), is( clonedRow ) );
  }

  @Test
  public void testRowIterator() throws Exception {
    CountDownLatch testLatch = new CountDownLatch( 1 );
    ServiceObserver serviceObserver = new ServiceObserver( executor ) {
      @Override public void run() {
        rowMetaAndData.add( new RowMetaAndData() );
        try {
          testLatch.await();
          rowMetaAndData.add( new RowMetaAndData() );
          rowMetaAndData.add( new RowMetaAndData() );
        } catch ( InterruptedException e ) {
          throw new RuntimeException( e );
        }
        isRunning = false;
      }
    };
    Executors.newSingleThreadExecutor().submit( serviceObserver );
    Iterator<RowMetaAndData> rows = serviceObserver.rows();
    assertTrue( rows.hasNext() );
    rows.next();
    Executors.newSingleThreadScheduledExecutor().schedule( testLatch::countDown, 10, TimeUnit.MILLISECONDS );
    assertTrue( rows.hasNext() );
    rows.next();
    assertTrue( rows.hasNext() );
    rows.next();
    assertFalse( rows.hasNext() );
    testLatch.countDown();
  }

  @Test
  public void cloneErrorIsPropogated()
    throws KettleValueException, ExecutionException, InterruptedException, KettleStepException {
    when( rowMeta.cloneRow( row ) ).thenThrow( exception );
    observer.run();
    verify( stepInterface ).addRowListener( rowAdapterCaptor.capture() );
    RowAdapter rowAdapter = rowAdapterCaptor.getValue();
    rowAdapter.rowWrittenEvent( rowMeta, row );
    try {
      observer.get();
      fail( "Expected exception" );
    } catch ( Exception e ) {
      assertThat( e.getCause(), is( exception ) );
    }
  }
}
