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

package org.pentaho.di.trans.dataservice.serialization;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.OptimizationImpactInfo;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownType;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

import javax.cache.Cache;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil.createCacheEntries;
import static org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil.createCacheKeys;

@RunWith( MockitoJUnitRunner.class )
public class DataServiceMetaStoreUtilTest {

  public static final String DATA_SERVICE_NAME = "DataServiceNameForLoad";
  public static final String DATA_SERVICE_STEP = "DataServiceStepNameForLoad";
  static final String OPTIMIZATION = "Optimization";
  static final String OPTIMIZED_STEP = "Optimized Step";
  static final String OPTIMIZATION_VALUE = "Optimization Value";
  private final ObjectId objectId = new StringObjectId( UUID.randomUUID().toString() );
  private TransMeta transMeta;
  private IMetaStore metaStore;
  private DataServiceMeta dataService;

  @Mock Cache<Integer, String> cache;
  @Mock( answer = Answers.RETURNS_DEEP_STUBS ) Repository repository;
  @Mock Function<Exception, Void> exceptionHandler;
  @Mock KettleException notFoundException;

  private DataServiceMetaStoreUtil metaStoreUtil;

  @BeforeClass
  public static void init() throws KettleException {
    KettleEnvironment.init();
  }

  @Before
  public void setUp() throws KettleException, MetaStoreException {
    LogChannel.GENERAL = mock( LogChannelInterface.class );
    doNothing().when( LogChannel.GENERAL ).logBasic( anyString() );

    transMeta = new TransMeta();
    transMeta.setName( "dataServiceTrans" );
    transMeta.setRepository( repository );
    when( repository.getRepositoryMeta().getRepositoryCapabilities().supportsReferences() ).thenReturn( true );
    transMeta.setObjectId( objectId );
    doThrow( notFoundException ).when( repository ).loadTransformation( any( ObjectId.class ), anyString() );
    doReturn( transMeta ).when( repository ).loadTransformation( objectId, null );

    metaStore = new MemoryMetaStore();
    this.dataService = createDataService( transMeta );

    PushDownFactory optimizationFactory = mock( PushDownFactory.class );
    when( (Class) optimizationFactory.getType() ).thenReturn( TestOptimization.class );
    when( optimizationFactory.createPushDown() ).then( new Answer<PushDownType>() {
      @Override public PushDownType answer( InvocationOnMock invocation ) throws Throwable {
        return new TestOptimization();
      }
    } );
    metaStoreUtil = new DataServiceMetaStoreUtil( ImmutableList.of( optimizationFactory ), cache );
  }

  private static DataServiceMeta createDataService( TransMeta transMeta ) {
    DataServiceMeta dataService = new DataServiceMeta( transMeta );
    dataService.setName( DATA_SERVICE_NAME );
    dataService.setStepname( DATA_SERVICE_STEP );
    PushDownOptimizationMeta optimization = new PushDownOptimizationMeta();
    optimization.setName( OPTIMIZATION );
    optimization.setStepName( OPTIMIZED_STEP );
    TestOptimization optimizationType = new TestOptimization();
    optimizationType.setValue( OPTIMIZATION_VALUE );
    optimization.setType( optimizationType );
    dataService.setPushDownOptimizationMeta( Lists.newArrayList( optimization ) );
    return dataService;
  }

  @Test public void testSaveLoad() throws Exception {
    metaStoreUtil.save( repository, metaStore, dataService );

    assertThat( metaStoreUtil.getDataService( DATA_SERVICE_NAME, transMeta ), validDataService() );
    assertThat( metaStoreUtil.getDataService( DATA_SERVICE_NAME, repository, metaStore ), validDataService() );

    assertThat( metaStoreUtil.getDataServiceByStepName( transMeta, DATA_SERVICE_STEP ), validDataService() );

    assertThat( metaStoreUtil.getDataServices( repository, metaStore, exceptionHandler ),
      contains( validDataService() ) );
    verify( exceptionHandler, never() ).apply( any( Exception.class ) );
    assertThat( metaStoreUtil.getDataServices( transMeta ), contains( validDataService() ) );
  }

  @Test public void testInaccessibleTransMeta() throws Exception {
    metaStoreUtil.save( repository, metaStore, dataService );

    TransMeta invalidTransMeta = new TransMeta();
    invalidTransMeta.setName( "brokenTrans" );
    invalidTransMeta.setObjectId( new StringObjectId( "Not In Repo" ) );
    invalidTransMeta.setRepository( repository );
    DataServiceMeta invalidDataService = createDataService( invalidTransMeta );
    invalidDataService.setName( "Invalid Data Service" );
    metaStoreUtil.save( repository, metaStore, invalidDataService );

    assertThat( metaStoreUtil.getDataServices( repository, metaStore, exceptionHandler ),
      contains( validDataService() ) );
    verify( exceptionHandler, atLeastOnce() ).apply( notFoundException );

    try {
      metaStoreUtil.getDataService( invalidDataService.getName(), repository, metaStore );
      fail( "Expected an exception" );
    } catch ( Exception e ) {
      assertThat( Throwables.getRootCause( e ), equalTo( (Throwable) notFoundException ) );
    }
    assertThat( metaStoreUtil.getDataService( DATA_SERVICE_NAME, repository, metaStore ), validDataService() );
  }

  @Test public void testStepCacheMiss() throws Exception {
    assertThat( metaStoreUtil.getDataServiceByStepName( transMeta, DATA_SERVICE_STEP ), nullValue() );
    verify( cache ).putAll( Maps.asMap( createCacheKeys( transMeta, DATA_SERVICE_STEP ), Functions.constant( "" ) ) );
  }

  @Test public void testStepCacheSave() throws Exception {
    metaStoreUtil.save( repository, metaStore, dataService );
    verify( cache ).putAll(
      Maps.asMap( createCacheKeys( transMeta, DATA_SERVICE_STEP ), Functions.constant( DATA_SERVICE_NAME ) ) );
  }

  @Test public void testStepCacheHit() throws Exception {
    Set<Integer> cacheKeys = createCacheKeys( transMeta, DATA_SERVICE_STEP );
    Map<Integer, String> cacheEntries = createCacheEntries( dataService );
    assertThat( cacheEntries.keySet(), equalTo( cacheKeys ) );

    metaStoreUtil.save( repository, metaStore, dataService );
    verify( cache ).putAll( cacheEntries );

    when( cache.getAll( cacheKeys ) ).thenReturn( cacheEntries );
    assertThat( metaStoreUtil.getDataServiceByStepName( transMeta, DATA_SERVICE_STEP ), validDataService() );

    metaStoreUtil.removeDataService( metaStore, dataService );
    verify( cache ).removeAll( cacheKeys );

    assertThat( metaStoreUtil.getDataServiceByStepName( transMeta, DATA_SERVICE_STEP ), nullValue() );
    verify( cache, times( cacheKeys.size() ) ).remove( argThat( in( cacheKeys ) ), eq( DATA_SERVICE_NAME ) );
    verify( cache ).putAll( argThat( hasEntry( in( cacheKeys ), emptyString() ) ) );
  }

  @Test public void testRemove() throws MetaStoreException {
    metaStoreUtil.save( repository, metaStore, dataService );
    metaStoreUtil.removeDataService( metaStore, dataService );

    assertThat( metaStoreUtil.getDataServices( repository, metaStore, exceptionHandler ), emptyIterable() );
    verify( exceptionHandler, never() ).apply( any( Exception.class ) );
  }

  private Matcher<DataServiceMeta> validDataService() {
    return allOf(
      hasProperty( "name", equalTo( DATA_SERVICE_NAME ) ),
      hasProperty( "stepname", equalTo( DATA_SERVICE_STEP ) ),
      hasProperty( "serviceTrans", sameInstance( transMeta ) ),
      hasProperty( "pushDownOptimizationMeta", contains( validPushDownOptimization() ) )
    );
  }

  private Matcher<PushDownOptimizationMeta> validPushDownOptimization() {
    return allOf(
      hasProperty( "name", equalTo( OPTIMIZATION ) ),
      hasProperty( "stepName", equalTo( OPTIMIZED_STEP ) ),
      hasProperty( "type", allOf(
        instanceOf( TestOptimization.class ),
        hasProperty( "value", equalTo( OPTIMIZATION_VALUE ) )
      ) )
    );
  }

  public static class TestOptimization implements PushDownType {

    public String getValue() {
      return value;
    }

    public void setValue( String value ) {
      this.value = value;
    }

    @MetaStoreAttribute String value;

    @Override public void init( TransMeta transMeta, DataServiceMeta dataService, PushDownOptimizationMeta optMeta ) {
    }

    @Override public boolean activate( DataServiceExecutor executor, StepInterface stepInterface ) {
      return false;
    }

    @Override public OptimizationImpactInfo preview( DataServiceExecutor executor, StepInterface stepInterface ) {
      return null;
    }

  }
}
