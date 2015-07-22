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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.OptimizationImpactInfo;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptTypeForm;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownType;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

import javax.cache.Cache;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class DataServiceMetaStoreUtilTest {

  public static final String DATA_SERVICE_NAME = "DataServiceNameForLoad";
  public static final String DATA_SERVICE_STEP = "DataServiceStepNameForLoad";
  static final String OPTIMIZATION = "Optimization";
  static final String OPTIMIZED_STEP = "Optimized Step";
  static final String OPTIMIZATION_VALUE = "Optimization Value";
  private final StringObjectId objectId = new StringObjectId( UUID.randomUUID().toString() );
  private TransMeta transMeta;
  private IMetaStore metaStore;
  private DataServiceMeta dataService;

  @Mock private Cache<String, DataServiceMeta> cache;
  @Mock( answer = Answers.RETURNS_DEEP_STUBS ) private Repository repository;

  private DataServiceMetaStoreUtil metaStoreUtil;
  private MetaStoreFactory<DataServiceMeta> embeddedMetaStore;

  @BeforeClass
  public static void init() throws KettleException {
    KettleEnvironment.init();
  }

  @Before
  public void setUp() throws KettleException, MetaStoreException {
    LogChannel.GENERAL = mock( LogChannelInterface.class );
    doNothing().when( LogChannel.GENERAL ).logBasic( anyString() );

    transMeta = new TransMeta();
    transMeta.setRepository( repository );
    when( repository.getRepositoryMeta().getRepositoryCapabilities().supportsReferences() ).thenReturn( true );
    transMeta.setObjectId( objectId );
    when( repository.loadTransformation( objectId, null ) ).thenReturn( transMeta );

    metaStore = new MemoryMetaStore();
    dataService = new DataServiceMeta( transMeta );
    dataService.setName( DATA_SERVICE_NAME );
    dataService.setStepname( DATA_SERVICE_STEP );
    PushDownOptimizationMeta optimization = new PushDownOptimizationMeta();
    optimization.setName( OPTIMIZATION );
    optimization.setStepName( OPTIMIZED_STEP );
    TestOptimization optimizationType = new TestOptimization();
    optimizationType.setValue( OPTIMIZATION_VALUE );
    optimization.setType( optimizationType );
    dataService.setPushDownOptimizationMeta( Lists.newArrayList( optimization ) );

    PushDownFactory optimizationFactory = new TestOptimizationFactory();
    metaStoreUtil = new DataServiceMetaStoreUtil( ImmutableList.of( optimizationFactory ), cache );
  }

  @Test public void testCacheSave() throws Exception {
    String transStepKey = DataServiceMeta.createCacheKey( transMeta, DATA_SERVICE_STEP );
    metaStoreUtil.save( metaStore, dataService );

    verify( cache ).putAll( argThat( allOf( aMapWithSize( 2 ),
      hasEntry( transStepKey, dataService ),
      hasEntry( DATA_SERVICE_NAME, dataService )
    ) ) );
  }

  @Test public void testCacheLoadByName() throws Exception {
    when( cache.get( DATA_SERVICE_NAME ) ).thenReturn( dataService );
    assertThat( metaStoreUtil.getDataService( DATA_SERVICE_NAME, mock( Repository.class ), mock( IMetaStore.class ) ),
      sameInstance( dataService ) );
  }

  @Test public void testCacheLoadByStep() throws Exception {
    String key = DataServiceMeta.createCacheKey( transMeta, DATA_SERVICE_STEP );

    when( cache.get( key ) ).thenReturn( dataService );
    assertThat( metaStoreUtil.getDataServiceByStepName( transMeta, DATA_SERVICE_STEP ),
      sameInstance( dataService ) );

    dataService.setStepname( "different step" );
    assertThat( metaStoreUtil.getDataServiceByStepName( transMeta, DATA_SERVICE_STEP ), nullValue() );
    verify( cache ).remove( key, dataService );
  }

  @Test public void testGetByName() throws MetaStoreException {
    metaStoreUtil.save( metaStore, dataService );

    verifyData( metaStoreUtil.getDataService( DATA_SERVICE_NAME, transMeta ) );
  }

  @Test public void testGetByStepName() throws MetaStoreException {
    metaStoreUtil.save( metaStore, dataService );

    verifyData( metaStoreUtil.getDataServiceByStepName( transMeta, DATA_SERVICE_STEP ) );
  }

  @Test public void testGetInMetaStore() throws MetaStoreException, KettleException {
    metaStoreUtil.save( metaStore, dataService );

    verifyData( metaStoreUtil.getDataService( DATA_SERVICE_NAME, repository, metaStore ) );
  }

  @Test public void testGetAllInMetaStore() throws MetaStoreException {
    metaStoreUtil.save( metaStore, dataService );

    List<DataServiceMeta> dataServices = metaStoreUtil.getDataServices( repository, metaStore );
    assertThat( dataServices, hasSize( 1 ) );
    verifyData( dataServices.get( 0 ) );
  }

  @Test public void testGetAllInTrans() throws MetaStoreException {
    metaStoreUtil.save( metaStore, dataService );

    List<DataServiceMeta> dataServices = metaStoreUtil.getDataServices( transMeta );
    assertThat( dataServices, hasSize( 1 ) );
    verifyData( dataServices.get( 0 ) );
  }

  @Test public void testRemove() throws MetaStoreException {
    metaStoreUtil.save( metaStore, dataService );
    metaStoreUtil.removeDataService( transMeta, metaStore, dataService );

    assertThat( metaStoreUtil.getDataServices( repository, metaStore ), empty() );
  }

  private void verifyData( DataServiceMeta dataServiceMeta ) {
    assertThat( dataServiceMeta, notNullValue() );
    assertEquals( DATA_SERVICE_NAME, dataServiceMeta.getName() );
    assertEquals( DATA_SERVICE_STEP, dataServiceMeta.getStepname() );
    assertThat( dataServiceMeta.getServiceTrans(), sameInstance( transMeta ) );
    List<PushDownOptimizationMeta> optimizations = dataServiceMeta.getPushDownOptimizationMeta();
    assertThat( optimizations, hasSize( 1 ) );
    verifyData( optimizations.get( 0 ) );
  }

  private void verifyData( PushDownOptimizationMeta pushDownOptimizationMeta ) {
    assertThat( pushDownOptimizationMeta.getName(), equalTo( OPTIMIZATION ) );
    assertThat( pushDownOptimizationMeta.getStepName(), equalTo( OPTIMIZED_STEP ) );
    assertThat( pushDownOptimizationMeta.getType(), instanceOf( TestOptimization.class ) );
    verifyData( ( (TestOptimization) pushDownOptimizationMeta.getType() ) );
  }

  private void verifyData( TestOptimization type ) {
    assertThat( type.getValue(), equalTo( OPTIMIZATION_VALUE ) );
  }

  private class TestOptimizationFactory implements PushDownFactory {
    @Override public String getName() {
      return OPTIMIZATION;
    }

    @Override public Class<? extends PushDownType> getType() {
      return TestOptimization.class;
    }

    @Override public PushDownType createPushDown() {
      return new TestOptimization();
    }

    @Override public PushDownOptTypeForm createPushDownOptTypeForm() {
      return mock( PushDownOptTypeForm.class );
    }
  }

  public final class TestOptimization implements PushDownType {

    public String getValue() {
      return value;
    }

    public void setValue( String value ) {
      this.value = value;
    }

    @MetaStoreAttribute String value;

    @Override public String getTypeName() {
      return OPTIMIZATION;
    }

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
