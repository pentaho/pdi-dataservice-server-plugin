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

package org.pentaho.di.trans.dataservice.optimization.cache;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.SqlTransGenerator;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownType;
import org.pentaho.di.trans.step.StepInterface;

import javax.cache.Cache;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.class )
public class ServiceCacheTest {

  static final String SERVICE_STEP = "SERVICE STEP";
  @Mock ServiceCacheFactory factory;
  @Mock( answer = Answers.RETURNS_DEEP_STUBS ) Trans genTrans;
  @Mock( answer = Answers.RETURNS_DEEP_STUBS ) Trans serviceTrans;
  @Mock SqlTransGenerator sqlTransGenerator;
  @Mock DataServiceMeta dataServiceMeta;
  @Mock Cache<CachedService.CacheKey, CachedService> cache;

  @InjectMocks ServiceCache serviceCache;
  RowMeta rowMeta;
  PushDownOptimizationMeta serviceCacheOpt;
  PushDownOptimizationMeta otherOpt;
  StepInterface serviceStep;

  @Before
  public void setUp() throws Exception {
    rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMeta( "ID", ValueMetaInterface.TYPE_STRING ) );
    rowMeta.addValueMeta( new ValueMeta( "A", ValueMetaInterface.TYPE_INTEGER ) );
    rowMeta.addValueMeta( new ValueMeta( "B", ValueMetaInterface.TYPE_INTEGER ) );

    when( factory.getCache( "MOCK_SERVICE" ) ).thenReturn( Optional.of( cache ) );
    when( factory.getCache( serviceCache, "MOCK_SERVICE" ) ).thenReturn( cache );
    when( dataServiceMeta.getStepname() ).thenReturn( SERVICE_STEP );
    serviceStep = serviceTrans.findRunThread( SERVICE_STEP );
    TransMeta transMeta = serviceTrans.getTransMeta();
    when( transMeta.getXML() ).thenReturn( "<transformation/>" );

    when( factory.getExecutorService() ).thenReturn( MoreExecutors.sameThreadExecutor() );

    serviceCacheOpt =
      when( mock( PushDownOptimizationMeta.class ).getType() ).thenReturn( mock( ServiceCache.class ) ).getMock();
    otherOpt =
      when( mock( PushDownOptimizationMeta.class ).getType() ).thenReturn( mock( PushDownType.class ) ).getMock();
  }

  @Test
  public void testActivateObserve() throws Exception {
    when( dataServiceMeta.getPushDownOptimizationMeta() ).thenReturn( ImmutableList.of( serviceCacheOpt, otherOpt ) );

    DataServiceExecutor executor = dataServiceExecutor( "SELECT * FROM MOCK_SERVICE ORDER BY ID" );
    CachedService.CacheKey key = CachedService.CacheKey.create( executor );
    CachedService cachedService = CachedService.complete( ImmutableList.<RowMetaAndData>of() );

    ServiceObserver observer = mock( ServiceObserver.class );
    when( factory.createObserver( executor ) ).thenReturn( observer );
    when( observer.install() ).thenReturn( Futures.immediateFuture( cachedService ) );

    when( cache.get( any( CachedService.CacheKey.class ) ) ).thenReturn( null );
    when( cache.putIfAbsent( key.withoutOrder(), cachedService ) ).thenReturn( true );
    assertThat( serviceCache.activate( executor, serviceStep ), is( false ) );

    verify( cache ).putIfAbsent( key.withoutOrder(), cachedService );
    verifyNoMoreInteractions( ignoreStubs( cache ) );
  }

  @Test
  public void testObserveWithoutCondition() throws Exception {
    String select = "SELECT * FROM MOCK_SERVICE";
    String selectWithCondition = select + " WHERE A = 2";

    // These queries should always ignore WHERE conditions
    for ( Map.Entry<String, List<PushDownOptimizationMeta>> testEntry : ImmutableMultimap
      .<String, List<PushDownOptimizationMeta>>builder()
      .put( select, ImmutableList.of( serviceCacheOpt ) )
      .put( select, ImmutableList.of( serviceCacheOpt, otherOpt ) )
      .put( selectWithCondition, ImmutableList.of( serviceCacheOpt ) )
      .put( selectWithCondition, ImmutableList.<PushDownOptimizationMeta>of() )
      .build().entries() ) {
      when( dataServiceMeta.getPushDownOptimizationMeta() ).thenReturn( testEntry.getValue() );

      DataServiceExecutor executor = dataServiceExecutor( testEntry.getKey() );
      CachedService cachedService = CachedService.complete( ImmutableList.<RowMetaAndData>of() );
      CachedService.CacheKey key = CachedService.CacheKey.create( executor ).withoutCondition();

      ServiceObserver observer = mock( ServiceObserver.class );
      when( factory.createObserver( executor ) ).thenReturn( observer );
      when( observer.install() ).thenReturn( Futures.immediateFuture( cachedService ) );

      when( cache.get( any( CachedService.CacheKey.class ) ) ).thenReturn( null );
      when( cache.putIfAbsent( key, cachedService ) ).thenReturn( true );
      assertThat( serviceCache.activate( executor, serviceStep ), is( false ) );

      try {
        verify( cache ).putIfAbsent( key, cachedService );
        verifyNoMoreInteractions( ignoreStubs( cache ) );
      } catch ( AssertionError e ) {
        throw new AssertionError( testEntry.toString(), e );
      }
      reset( (Cache) cache );
    }
  }

  @Test
  public void testObserveUpdateExisting() throws Exception {
    when( dataServiceMeta.getPushDownOptimizationMeta() ).thenReturn( ImmutableList.of( serviceCacheOpt, otherOpt ) );

    DataServiceExecutor executor = dataServiceExecutor( "SELECT * FROM MOCK_SERVICE WHERE A = 2" );
    CachedService.CacheKey key = CachedService.CacheKey.create( executor );

    CachedService existingCache = mock( CachedService.class );
    when( cache.get( key.withoutCondition() ) ).thenReturn( null );
    when( cache.get( key ) ).thenReturn( existingCache );
    when( existingCache.answersQuery( executor ) ).thenReturn( false );

    ServiceObserver observer = mock( ServiceObserver.class );
    when( factory.createObserver( executor ) ).thenReturn( observer );
    CachedService cachedService = CachedService.complete( ImmutableList.<RowMetaAndData>of() );
    when( observer.install() ).thenReturn( Futures.immediateFuture( cachedService ) );

    when( cache.putIfAbsent( key, cachedService ) ).thenReturn( false );
    when( cache.replace( key, existingCache, cachedService ) ).thenReturn( true );
    assertThat( serviceCache.activate( executor, serviceStep ), is( false ) );

    verify( cache ).replace( key, existingCache, cachedService );
    verifyNoMoreInteractions( ignoreStubs( cache ) );
  }

  @Test
  public void testReplay() throws Exception {
    when( dataServiceMeta.getPushDownOptimizationMeta() ).thenReturn( ImmutableList.of( serviceCacheOpt, otherOpt ) );

    DataServiceExecutor executor = dataServiceExecutor( "SELECT * FROM MOCK_SERVICE WHERE A = 2" );
    CachedService.CacheKey key = CachedService.CacheKey.create( executor );
    CachedService cachedService = mock( CachedService.class );
    CachedServiceLoader cachedServiceLoader = mock( CachedServiceLoader.class );

    when( cache.get( key ) ).thenReturn( cachedService );
    when( cachedService.answersQuery( executor ) ).thenReturn( true );
    when( factory.createCachedServiceLoader( cachedService ) ).thenReturn( cachedServiceLoader );
    when( cachedServiceLoader.replay( executor ) ).thenReturn( Futures.immediateFuture( 2000 ) );

    assertThat( serviceCache.activate( executor, serviceStep ), is( true ) );
    verify( cachedServiceLoader ).replay( executor );
  }

  @Test
  public void testReplayComplete() throws Exception {
    when( dataServiceMeta.getPushDownOptimizationMeta() ).thenReturn( ImmutableList.of( serviceCacheOpt, otherOpt ) );

    DataServiceExecutor executor = dataServiceExecutor( "SELECT * FROM MOCK_SERVICE WHERE A = 2" );
    CachedService.CacheKey key = CachedService.CacheKey.create( executor );
    CachedService cachedService = mock( CachedService.class );
    final CachedServiceLoader cachedServiceLoader = mock( CachedServiceLoader.class );

    when( cache.get( key ) ).thenReturn( null );
    when( cache.get( key.withoutCondition() ) ).thenReturn( cachedService );
    when( cachedService.isComplete() ).thenReturn( true );
    when( factory.createCachedServiceLoader( cachedService ) ).thenReturn( cachedServiceLoader );
    when( cachedServiceLoader.replay( executor ) ).thenReturn( Futures.immediateFuture( 2000 ) );

    assertThat( serviceCache.activate( executor, serviceStep ), is( true ) );
    verify( cachedServiceLoader ).replay( executor );
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
}
