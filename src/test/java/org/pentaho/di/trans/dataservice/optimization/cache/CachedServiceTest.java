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

package org.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.SqlTransGenerator;
import org.pentaho.di.trans.dataservice.optimization.cache.CachedService.CacheKey;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepListener;
import org.pentaho.di.trans.step.StepMeta;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.class )
public class CachedServiceTest {

  public static final String INJECTOR_STEP = "INJECTOR_STEP";
  public static final String OUTPUT = "OUTPUT";
  @Mock( answer = Answers.RETURNS_DEEP_STUBS ) Trans genTrans;
  @Mock( answer = Answers.RETURNS_DEEP_STUBS ) Trans serviceTrans;
  @Mock StepInterface serviceStep;
  @Mock SqlTransGenerator sqlTransGenerator;
  @Mock DataServiceMeta dataServiceMeta;

  private List<RowMetaAndData> testData;
  private RowMeta rowMeta;

  @Before
  public void setUp() throws Exception {
    rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMeta( "ID", ValueMetaInterface.TYPE_STRING ) );
    rowMeta.addValueMeta( new ValueMeta( "A", ValueMetaInterface.TYPE_INTEGER ) );
    rowMeta.addValueMeta( new ValueMeta( "B", ValueMetaInterface.TYPE_INTEGER ) );

    testData = Lists.newArrayListWithExpectedSize( 100 );
    for ( long i = 0; i < 100; i++ ) {
      testData.add( new RowMetaAndData( rowMeta, String.valueOf( i ), i % 13, i % 17 ) );
    }

    when( dataServiceMeta.getStepname() ).thenReturn( "service step" );
    when( serviceTrans.findRunThread( "service step" ) ).thenReturn( serviceStep );
    when( serviceStep.getTrans() ).thenReturn( serviceTrans );
    when( sqlTransGenerator.getInjectorStepName() ).thenReturn( INJECTOR_STEP );
  }

  @Test
  public void testCreateCacheKey() throws Exception {
    CacheKey unbounded, withCondition, withConditionOrdered, withLimit, otherService;

    unbounded = cacheKey( "SELECT * from MOCK_SERVICE" );
    withCondition = cacheKey( "SELECT * from MOCK_SERVICE WHERE A = 42" );
    withConditionOrdered = cacheKey( "SELECT * from MOCK_SERVICE WHERE A=42 ORDER BY B" );
    withLimit = cacheKey( "SELECT * from MOCK_SERVICE LIMIT 20" );
    otherService = cacheKey( "SELECT * from OTHER_SERVICE" );

    // Verifies order from most specific to general
    assertThat( withConditionOrdered.all(), contains( withConditionOrdered, withCondition, unbounded ) );
    assertThat( withLimit, equalTo( unbounded ) );
    for ( CacheKey cacheKey : ImmutableList.of( withCondition, withConditionOrdered, otherService ) ) {
      assertThat( cacheKey, not( equalTo( unbounded ) ) );
    }
    assertThat( withCondition.withoutCondition(), equalTo( unbounded ) );
    assertThat( withConditionOrdered.withoutOrder(), equalTo( withCondition ) );

    // Verify that execution parameters will change the key
    SQL sql = new SQL( "SELECT * FROM MOCK_SERVICE" );
    sql.parse( rowMeta );

    assertThat( CacheKey.create( new DataServiceExecutor.Builder( sql, dataServiceMeta )
        .prepareExecution( false )
        .parameters( ImmutableMap.of( "foo", "bar" ) )
        .build() ),
      not( equalTo( CacheKey.create( new DataServiceExecutor.Builder( sql, dataServiceMeta )
        .prepareExecution( false )
        .build() ) ) ) );
  }

  @Test
  public void testPartial() throws Exception {
    CachedService rowLimit, limit, limitOffset, unlimited;

    when( sqlTransGenerator.getRowLimit() ).thenReturn( 10, 0 );
    rowLimit = partial( dataServiceExecutor( "SELECT * FROM MOCK_SERVICE" ) );

    limit = partial( dataServiceExecutor(
      "SELECT * FROM MOCK_SERVICE LIMIT 20"
    ) );

    limitOffset = partial( dataServiceExecutor(
      "SELECT * FROM MOCK_SERVICE LIMIT 20 OFFSET 10"
    ) );

    // Not sure if an unlimited partial result is possible, but just in case
    unlimited = partial( dataServiceExecutor( "SELECT * FROM MOCK_SERVICE" ) );

    for ( CachedService loader : ImmutableList.of( rowLimit, limit, limitOffset ) ) {
      assertThat( loader.getRowMetaAndData(), equalTo( testData ) );
      assertThat( loader.getRanking().get(), lessThan( Integer.MAX_VALUE ) );
    }
    assertThat( unlimited.getRowMetaAndData(), equalTo( testData ) );
    assertThat( unlimited.getRanking().get(), equalTo( Integer.MAX_VALUE ) );

    assertThat( rowLimit.getRanking().get(), lessThan( limit.getRanking().get() ) );
    assertThat( limit.getRanking().get(), lessThan( limitOffset.getRanking().get() ) );
    assertThat( limitOffset.getRanking().get(), lessThan( unlimited.getRanking().get() ) );
  }

  @Test
  public void testComplete() throws Exception {
    CachedService unbounded, rowLimit, ordered;

    unbounded = CachedService.complete( testData );

    when( sqlTransGenerator.getRowLimit() ).thenReturn( 10, 0 );
    rowLimit = CachedService.complete( testData );

    ordered = CachedService.complete( testData );

    for ( CachedService loader : ImmutableList.of( unbounded, rowLimit, ordered ) ) {
      assertThat( loader.getRowMetaAndData(), equalTo( testData ) );
      assertThat( loader.isComplete(), is( true ) );
    }
  }

  @Test
  public void testObserve() throws Exception {
    DataServiceExecutor executor = dataServiceExecutor( "SELECT * FROM MOCK_SERVICE" );
    ServiceObserver observer = new ServiceObserver( executor );

    assertThat( observer.install(), sameInstance( (ListenableFuture<CachedService>) observer ) );
    assertThat( executor.getListenerMap().get( DataServiceExecutor.ExecutionPoint.READY ), hasItem( observer ) );

    observer.run();

    ArgumentCaptor<RowListener> rowListener = ArgumentCaptor.forClass( RowListener.class );
    ArgumentCaptor<StepListener> stepListener = ArgumentCaptor.forClass( StepListener.class );

    verify( serviceStep ).addRowListener( rowListener.capture() );
    verify( serviceStep ).addStepListener( stepListener.capture() );
    assertThat( observer.isDone(), is( false ) );

    for ( RowMetaAndData metaAndData : testData ) {
      rowListener.getValue().rowWrittenEvent( metaAndData.getRowMeta(), metaAndData.getData() );
    }
    stepListener.getValue().stepFinished( serviceTrans, mock( StepMeta.class ), serviceStep );

    assertThat( observer.isDone(), is( true ) );
    assertThat( observer.get().getRowMetaAndData(), equalTo( (Iterable<RowMetaAndData>) testData ) );
    assertThat( observer.get().isComplete(), is( true ) );
  }

  @Test
  public void testObserveCancelled() throws Exception {
    DataServiceExecutor executor = dataServiceExecutor( "SELECT * FROM MOCK_SERVICE" );
    ServiceObserver observer = new ServiceObserver( executor );

    observer.install();
    assertThat( executor.getListenerMap().get( DataServiceExecutor.ExecutionPoint.READY ), hasItem( observer ) );

    ListenableFuture<CachedService> failedObserver = new ServiceObserver( executor ).install();
    try {
      failedObserver.get( 1, TimeUnit.SECONDS );
      fail( "Expecting an IllegalStateException" );
    } catch ( ExecutionException e ) {
      assertThat( e.getCause(), instanceOf( IllegalStateException.class ) );
    }
    assertThat( observer.isDone(), is( false ) );

    ArgumentCaptor<StepListener> stepListener = ArgumentCaptor.forClass( StepListener.class );
    observer.run();
    verify( serviceStep ).addStepListener( stepListener.capture() );

    when( genTrans.getErrors() ).thenReturn( 1 );
    stepListener.getValue().stepFinished( serviceTrans, mock( StepMeta.class ), serviceStep );

    try {
      observer.get( 1, TimeUnit.SECONDS );
      fail( "Expecting an KettleException" );
    } catch ( ExecutionException e ) {
      assertThat( e.getCause(), instanceOf( KettleException.class ) );
    }
  }

  @Test
  public void testObservePartial() throws Exception {
    DataServiceExecutor executor = dataServiceExecutor( "SELECT * FROM MOCK_SERVICE LIMIT 20" );
    ServiceObserver observer = new ServiceObserver( executor );

    observer.install();
    assertThat( executor.getListenerMap().get( DataServiceExecutor.ExecutionPoint.READY ), hasItem( observer ) );
    observer.run();

    ArgumentCaptor<RowListener> rowListener = ArgumentCaptor.forClass( RowListener.class );
    ArgumentCaptor<StepListener> stepListener = ArgumentCaptor.forClass( StepListener.class );

    verify( serviceStep ).addRowListener( rowListener.capture() );
    verify( serviceStep ).addStepListener( stepListener.capture() );
    assertThat( observer.isDone(), is( false ) );

    for ( RowMetaAndData metaAndData : FluentIterable.from( testData ) ) {
      rowListener.getValue().rowWrittenEvent( metaAndData.getRowMeta(), metaAndData.getData() );
    }
    when( serviceStep.isStopped() ).thenReturn( true );
    stepListener.getValue().stepFinished( serviceTrans, mock( StepMeta.class ), serviceStep );

    assertThat( observer.isDone(), is( true ) );
    CachedService cachedService = observer.get( 5, TimeUnit.SECONDS );
    assertThat( cachedService.isComplete(), is( false ) );
    assertThat( cachedService.getRowMetaAndData(), equalTo( testData ) );
  }

  @Test
  public void testReplayFullCache() throws Exception {
    DataServiceExecutor executor = dataServiceExecutor( "SELECT * FROM MOCK_SERVICE" );
    CachedService cachedService = CachedService.complete( testData );
    RowProducer rowProducer = genTrans.addRowProducer( INJECTOR_STEP, 0 );

    // Activate cachedServiceLoader
    Executor mockExecutor = mock( Executor.class );
    final CachedServiceLoader cachedServiceLoader = new CachedServiceLoader( cachedService, mockExecutor );
    ListenableFuture<Integer> replay = cachedServiceLoader.replay( executor );
    ArgumentCaptor<Runnable> replayRunnable = ArgumentCaptor.forClass( Runnable.class );
    verify( mockExecutor ).execute( replayRunnable.capture() );

    // Simulate executing data service
    executor.executeListeners( DataServiceExecutor.ExecutionPoint.READY );
    executor.executeListeners( DataServiceExecutor.ExecutionPoint.START );

    // Verify that serviceTrans never started, genTrans is accepting rows
    verify( serviceTrans ).killAll();
    verify( serviceTrans, never() ).startThreads();
    verify( genTrans ).startThreads();

    when(
      rowProducer.putRowWait( any( RowMetaInterface.class ), any( Object[].class ), anyInt(), any( TimeUnit.class ) )
    ).then( new Answer<Boolean>() {
      int calls = 0;

      @Override public Boolean answer( InvocationOnMock invocation ) throws Throwable {
        // Simulate full row set on tenth call
        return ++calls != 10;
      }
    } );
    when( genTrans.isRunning() ).thenReturn( true );

    // Run cache loader (would be asynchronous)
    replayRunnable.getValue().run();

    assertThat( replay.get( 1, TimeUnit.SECONDS ), equalTo( testData.size() ) );

    InOrder rowsProduced = inOrder( rowProducer );
    for ( int i = 0; i < testData.size(); i++ ) {
      RowMetaAndData metaAndData = testData.get( i );
      // Tenth row was called twice, since row set was full
      rowsProduced.verify( rowProducer, times( i == 9 ? 2 : 1 ) )
        .putRowWait( eq( metaAndData.getRowMeta() ), eq( metaAndData.getData() ), anyInt(), any( TimeUnit.class ) );
    }
    rowsProduced.verify( rowProducer ).finished();
    rowsProduced.verifyNoMoreInteractions();
  }

  @Test
  public void testReplayPartialCache() throws Exception {
    DataServiceExecutor executor = dataServiceExecutor( "SELECT * FROM MOCK_SERVICE LIMIT 20" );
    CachedService cachedService = CachedService.complete( testData );
    RowProducer rowProducer = genTrans.addRowProducer( INJECTOR_STEP, 0 );

    // Activate cachedServiceLoader
    Executor mockExecutor = mock( Executor.class );
    final CachedServiceLoader cachedServiceLoader = new CachedServiceLoader( cachedService, mockExecutor );
    ListenableFuture<Integer> replay = cachedServiceLoader.replay( executor );
    ArgumentCaptor<Runnable> replayRunnable = ArgumentCaptor.forClass( Runnable.class );
    verify( mockExecutor ).execute( replayRunnable.capture() );

    // Simulate executing data service
    executor.executeListeners( DataServiceExecutor.ExecutionPoint.READY );
    executor.executeListeners( DataServiceExecutor.ExecutionPoint.START );

    // Verify that serviceTrans never started, genTrans is accepting rows
    verify( serviceTrans ).killAll();
    verify( serviceTrans, never() ).startThreads();
    verify( genTrans ).startThreads();

    final AtomicInteger rowsProduced = new AtomicInteger( 0 );
    when(
      rowProducer.putRowWait( any( RowMetaInterface.class ), any( Object[].class ), anyInt(), any( TimeUnit.class ) )
    ).then( new Answer<Boolean>() {
      @Override public Boolean answer( InvocationOnMock invocation ) throws Throwable {
        rowsProduced.getAndIncrement();
        return true;
      }
    } );
    when( genTrans.isRunning() ).then( new Answer<Boolean>() {
      @Override public Boolean answer( InvocationOnMock invocation ) throws Throwable {
        return rowsProduced.get() < 20;
      }
    } );

    // Run cache loader (would be asynchronous)
    replayRunnable.getValue().run();

    verify( rowProducer ).finished();
    assertThat( replay.get( 1, TimeUnit.SECONDS ), equalTo( 20 ) );
    assertThat( rowsProduced.get(), equalTo( 20 ) );

    for ( RowMetaAndData metaAndData : Iterables.limit( testData, 20 ) ) {
      verify( rowProducer )
        .putRowWait( eq( metaAndData.getRowMeta() ), eq( metaAndData.getData() ), anyInt(), any( TimeUnit.class ) );
    }
  }

  @Test
  public void testAnswersQuery() throws Exception {
    String query = "SELECT ID, A FROM mock_service" + " WHERE B = 1";
    String limit = " LIMIT 20";
    String offset = " OFFSET 10";
    String groupBy = " GROUP BY A";

    DataServiceExecutor executor = dataServiceExecutor( query + limit + offset );

    CachedService complete = CachedService.complete( testData );
    CachedService partial = partial( executor );

    // An identical query always works
    assertThat( complete.answersQuery( executor ), is( true ) );
    assertThat( partial.answersQuery( executor ), is( true ) );

    // A partial will answer a query with fewer requested rows
    assertThat( partial.answersQuery( dataServiceExecutor( query + limit ) ), is( true ) );
    when( sqlTransGenerator.getRowLimit() ).thenReturn( 10 );
    assertThat( partial.answersQuery( dataServiceExecutor( query ) ), is( true ) );
    when( sqlTransGenerator.getRowLimit() ).thenReturn( 0 );
    assertThat( partial.answersQuery( dataServiceExecutor( query ) ), is( false ) );

    // Only complete sets will answer a GROUP BY query
    assertThat( partial.answersQuery( dataServiceExecutor( query + groupBy ) ), is( false ) );
    assertThat( complete.answersQuery( dataServiceExecutor( query + groupBy ) ), is( true ) );
  }

  private CachedService partial( DataServiceExecutor executor ) {
    return CachedService.partial( testData, executor );
  }

  private DataServiceExecutor dataServiceExecutor( String query ) throws KettleException {
    SQL sql = new SQL( query );
    sql.parse( rowMeta );
    return new DataServiceExecutor.Builder( sql, dataServiceMeta )
      .sqlTransGenerator( sqlTransGenerator )
      .serviceTrans( serviceTrans )
      .genTrans( genTrans )
      .build();
  }

  private CacheKey cacheKey( String query ) throws KettleException {
    return CacheKey.create( dataServiceExecutor( query ) );
  }

}
