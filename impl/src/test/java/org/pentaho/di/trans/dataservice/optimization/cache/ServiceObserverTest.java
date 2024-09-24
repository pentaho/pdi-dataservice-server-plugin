/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
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
import org.mockito.junit.MockitoJUnitRunner;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.StrictStubs.class)
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
    CountDownLatch delayRowsLatch = new CountDownLatch( 1 );
    CountDownLatch firstRowLatch = new CountDownLatch( 1 );
    ServiceObserver serviceObserver = new ServiceObserver( executor ) {
      @Override public void run() {
        rowMetaAndData.add( new RowMetaAndData() );
        firstRowLatch.countDown();
        try {
          delayRowsLatch.await();
          rowMetaAndData.add( new RowMetaAndData() );
          latch.countDown();
          rowMetaAndData.add( new RowMetaAndData() );
          latch.countDown();
        } catch ( InterruptedException e ) {
          throw new RuntimeException( e );
        }
        isRunning = false;
      }
    };
    Executors.newSingleThreadExecutor().submit( serviceObserver );
    firstRowLatch.await();
    Iterator<RowMetaAndData> rows = serviceObserver.rows();
    assertTrue( rows.hasNext() );
    rows.next();
    Executors.newSingleThreadScheduledExecutor().schedule( delayRowsLatch::countDown, 1100, TimeUnit.MILLISECONDS );
    assertTrue( rows.hasNext() );
    rows.next();
    assertTrue( rows.hasNext() );
    rows.next();
    assertFalse( rows.hasNext() );
    delayRowsLatch.countDown();
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
