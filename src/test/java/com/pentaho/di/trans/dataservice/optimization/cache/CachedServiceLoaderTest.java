package com.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.pentaho.di.trans.dataservice.DataServiceExecutor;
import com.pentaho.di.trans.dataservice.DataServiceMeta;
import com.pentaho.di.trans.dataservice.SqlTransGenerator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.RowMetaAndData;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.class )
public class CachedServiceLoaderTest {

  public static final String INJECTOR_STEP = "INJECTOR_STEP";
  public static final String OUTPUT = "OUTPUT";
  @Mock( answer = Answers.RETURNS_DEEP_STUBS ) Trans genTrans;
  @Mock( answer = Answers.RETURNS_DEEP_STUBS ) Trans serviceTrans;
  @Mock SqlTransGenerator sqlTransGenerator;
  private List<RowMetaAndData> testData;
  private RowMeta rowMeta;

  @Before
  public void setUp() throws Exception {
    rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMeta( "ID", ValueMetaInterface.TYPE_STRING ) );

    testData = Lists.newArrayListWithExpectedSize( 100 );
    for ( int i = 0; i < 100; i++ ) {
      testData.add( new RowMetaAndData( rowMeta, String.valueOf( i ) ) );
    }
  }

  @Test
  public void testCreateCacheKey() throws Exception {
    DataServiceExecutor executor = mock( DataServiceExecutor.class );
    SQL sql = mock( SQL.class );

    final String service = "MOCK_SERVICE";
    final String whereClause = "SOME_CONDITION = TRUE";
    CachedServiceLoader.CacheKey cacheKey = new CachedServiceLoader.CacheKey( service, whereClause );

    when( executor.getSql() ).thenReturn( sql );
    when( executor.getServiceName() ).thenReturn( service );
    when( sql.getWhereClause() ).thenReturn( whereClause );
    CachedServiceLoader.CacheKey equivalentCacheKey = CachedServiceLoader.createCacheKey( executor );
    assertThat( cacheKey, equalTo( equivalentCacheKey ) );
    assertThat( cacheKey.hashCode(), equalTo( equivalentCacheKey.hashCode() ) );

    assertThat( cacheKey, not( equalTo( new CachedServiceLoader.CacheKey( service, null ) ) ) );
    assertThat( cacheKey, not( equalTo( new CachedServiceLoader.CacheKey( "OTHER_SERVICE", whereClause ) ) ) );
  }

  @Test
  public void testObserve() throws Exception {
    StepInterface serviceStep = mock( StepInterface.class );

    ArgumentCaptor<RowListener> rowListener = ArgumentCaptor.forClass( RowListener.class );
    ArgumentCaptor<StepListener> stepListener = ArgumentCaptor.forClass( StepListener.class );

    Future<CachedServiceLoader> future = CachedServiceLoader.observe( serviceStep );
    verify( serviceStep ).addRowListener( rowListener.capture() );
    verify( serviceStep ).addStepListener( stepListener.capture() );
    assertThat( future.isDone(), is( false ) );

    for ( RowMetaAndData metaAndData : testData ) {
      rowListener.getValue().rowWrittenEvent( metaAndData.getRowMeta(), metaAndData.getData() );
    }
    stepListener.getValue().stepFinished( serviceTrans, mock( StepMeta.class ), serviceStep );

    assertThat( future.isDone(), is( true ) );
    assertThat( future.get().getRowMetaAndData(), equalTo( (Iterable<RowMetaAndData>) testData ) );
  }

  @Test( expected = CancellationException.class )
  public void testObserveCancelled() throws Exception {
    StepInterface serviceStep = mock( StepInterface.class );

    ArgumentCaptor<RowListener> rowListener = ArgumentCaptor.forClass( RowListener.class );
    ArgumentCaptor<StepListener> stepListener = ArgumentCaptor.forClass( StepListener.class );

    Future<CachedServiceLoader> future = CachedServiceLoader.observe( serviceStep );
    verify( serviceStep ).addRowListener( rowListener.capture() );
    verify( serviceStep ).addStepListener( stepListener.capture() );
    assertThat( future.isDone(), is( false ) );

    for ( RowMetaAndData metaAndData : FluentIterable.from( testData ).limit( 15 ) ) {
      rowListener.getValue().rowWrittenEvent( metaAndData.getRowMeta(), metaAndData.getData() );
    }
    when( serviceStep.isStopped() ).thenReturn( true );
    stepListener.getValue().stepFinished( serviceTrans, mock( StepMeta.class ), serviceStep );

    assertThat( future.isDone(), is( true ) );
    assertThat( future.isCancelled(), is( true ) );
    future.get( 5, TimeUnit.SECONDS );
  }

  @Test
  public void testReplayFullCache() throws Exception {
    CachedServiceLoader cachedServiceLoader = new CachedServiceLoader( testData );
    DataServiceExecutor executor = new DataServiceExecutor.Builder(
      new SQL( "SELECT * FROM MOCK_SERVICE" ), mock( DataServiceMeta.class ) )
      .sqlTransGenerator( sqlTransGenerator )
      .genTrans( genTrans )
      .serviceTrans( serviceTrans )
      .build();

    when( sqlTransGenerator.getInjectorStepName() ).thenReturn( INJECTOR_STEP );
    RowProducer rowProducer = mock( RowProducer.class );
    when( genTrans.addRowProducer( INJECTOR_STEP, 0 ) ).thenReturn( rowProducer );

    verify( serviceTrans ).prepareExecution( null );
    verify( genTrans ).prepareExecution( null );
    doAnswer( new Answer() {
      int i = 0;

      @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
        Object[] data = (Object[]) invocation.getArguments()[1];
        if ( i == 0 ) {
          verify( serviceTrans ).killAll();
          verify( genTrans ).startThreads();
          verify( genTrans, times( 1 ) ).addRowProducer( eq( INJECTOR_STEP ), anyInt() );
        }
        assertThat( data, equalTo( testData.get( i ).getData() ) );

        i++;
        return null;
      }
    } ).when( rowProducer ).putRow( eq( rowMeta ), any( Object[].class ) );

    ListenableFuture<Integer> rowCount = cachedServiceLoader.replay( executor );
    executor.executeQuery( mock( RowListener.class ) );

    assertThat( rowCount.get( 5, TimeUnit.SECONDS ), is( 100 ) );
    verify( rowProducer, times( 100 ) ).putRow( any( RowMetaInterface.class ), any( Object[].class ) );
    verify( rowProducer ).finished();
  }

  @Test
  public void testReplayPartialCache() throws Exception {
    CachedServiceLoader cachedServiceLoader = new CachedServiceLoader( testData );
    DataServiceExecutor executor = new DataServiceExecutor.Builder(
      new SQL( "SELECT * FROM MOCK_SERVICE LIMIT 32" ), mock( DataServiceMeta.class ) )
      .sqlTransGenerator( sqlTransGenerator )
      .genTrans( genTrans )
      .serviceTrans( serviceTrans )
      .build();

    when( sqlTransGenerator.getInjectorStepName() ).thenReturn( INJECTOR_STEP );
    RowProducer rowProducer = mock( RowProducer.class );
    when( genTrans.addRowProducer( INJECTOR_STEP, 0 ) ).thenReturn( rowProducer );

    when( sqlTransGenerator.getResultStepName() ).thenReturn( OUTPUT );
    final ArgumentCaptor<StepListener> stepListener = ArgumentCaptor.forClass( StepListener.class );
    final StepInterface outputStep = genTrans.findRunThread( OUTPUT );

    verify( serviceTrans ).prepareExecution( null );
    verify( genTrans ).prepareExecution( null );

    ListenableFuture<Integer> rowCount = cachedServiceLoader.replay( executor );
    verify( outputStep ).addStepListener( stepListener.capture() );

    doAnswer( new Answer() {
      int i = 0;

      @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
        Object[] data = (Object[]) invocation.getArguments()[1];
        assertThat( data, equalTo( testData.get( i ).getData() ) );

        switch ( i ) {
          case 0:
            verify( serviceTrans ).killAll();
            verify( genTrans ).startThreads();
            verify( genTrans, times( 1 ) ).addRowProducer( eq( INJECTOR_STEP ), anyInt() );
            break;
          case 32:
            stepListener.getValue().stepFinished( genTrans, outputStep.getStepMeta(), outputStep );
            break;
          case 33:
            fail( "No more rows should be injected" );
        }

        i++;
        return null;
      }
    } ).when( rowProducer ).putRow( eq( rowMeta ), any( Object[].class ) );

    executor.executeQuery( mock( RowListener.class ) );

    assertThat( rowCount.get( 15, TimeUnit.SECONDS ), lessThan( 100 ) );
    verify( rowProducer, times( 33 ) ).putRow( any( RowMetaInterface.class ), any( Object[].class ) );
    verify( rowProducer ).finished();
  }

}
