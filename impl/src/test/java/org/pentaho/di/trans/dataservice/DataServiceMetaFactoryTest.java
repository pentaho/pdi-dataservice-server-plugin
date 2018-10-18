/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.optimization.cache.ServiceCache;
import org.pentaho.di.trans.dataservice.optimization.cache.ServiceCacheFactory;
import org.pentaho.di.trans.dataservice.resolvers.TransientResolver;
import org.pentaho.di.trans.dataservice.serialization.SynchronizationListener;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.spoon.Spoon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class DataServiceMetaFactoryTest {
  DataServiceMetaFactory factory;

  @Mock Context dataServiceContext;
  @Mock DataServiceDelegate dataServiceDelegate;
  @Mock Spoon mockSpoon;
  @Mock TransMeta transMeta;
  @Mock Repository repository;
  @Mock StepMeta stepMeta;
  @Mock SynchronizationListener synchronizationListener;
  @Mock ServiceCacheFactory cacheFactory;
  @Mock private Spoon spoon;
  @Mock private TransMeta activeTransMeta;

  String transName = "Test Trans Name";
  String stepName = "Test Step Name";

  @Before
  public void setup() {
    factory = spy( new DataServiceMetaFactory( () -> spoon ) );
    factory.setCacheFactory( cacheFactory );

    when( dataServiceContext.getDataServiceDelegate() ).thenReturn( dataServiceDelegate );
    when( dataServiceDelegate.createSyncService() ).thenReturn( synchronizationListener );
    when( dataServiceDelegate.getSpoon() ).thenReturn( mockSpoon );

    when( transMeta.getName() ).thenReturn( transName );
    when( cacheFactory.createPushDown() ).thenReturn( new ServiceCache( cacheFactory ) );

    when( stepMeta.getName() ).thenReturn( stepName );
    when( stepMeta.getParentTransMeta() ).thenReturn( transMeta );
    when( spoon.getActiveTransformation() ).thenReturn( activeTransMeta );
  }

  @Test
  public void testCreateDataServiceSpoon() throws Exception {
    when( mockSpoon.getRepository() ).thenReturn( repository );
    testCreateDataServiceCommon();
  }

  @Test
  public void testCreateDataServiceNonSpoon() throws Exception {
    factory = spy( new DataServiceMetaFactory() );
    factory.setCacheFactory( cacheFactory );
    testCreateDataServiceCommon();
  }

  @Test
  public void testCreateDataServiceLocal() throws Exception {
    when( mockSpoon.getRepository() ).thenReturn( repository );
    when( spoon.getActiveTransformation() ).thenReturn( transMeta );
    testCreateDataServiceCommon();
  }

  private void testCreateDataServiceCommon() throws Exception {
    when( mockSpoon.getRepository() ).thenReturn( repository );

    DataServiceMeta ds = factory.createDataService( stepMeta );
    assertNotNull( ds );
    assertTrue( TransientResolver.isTransient( ds.getName() ) );
    assertEquals( transMeta, ds.getServiceTrans() );
    assertEquals( stepName, ds.getStepname() );
    assertEquals( 1, ds.getPushDownOptimizationMeta().size() );
    assertTrue( ds.getPushDownOptimizationMeta().get( 0 ).getType() instanceof ServiceCache );
    assertEquals( stepName, ds.getPushDownOptimizationMeta().get( 0 ).getStepName() );
    assertEquals( 0, ds.getRowLimit() );
    verify( factory, times( 1 ) ).createDataService( eq( stepMeta ), eq( null ) );

    int rowLimit = 1000;
    ds = factory.createDataService( stepMeta, rowLimit );
    assertEquals( rowLimit, ds.getRowLimit() );
  }

  @Test
  public void testCreateDataServiceName() throws Exception {
    when( transMeta.getObjectId() ).thenReturn( null );
    when( transMeta.getFilename() ).thenReturn( "C:\\windows\\path\\file.ktr" );
    DataServiceMeta ds1 = factory.createDataService( stepMeta );
    assertTrue( TransientResolver.isTransient( ds1.getName() ) );

    when( transMeta.getFilename() ).thenReturn( "/unix/path/file.ktr" );
    DataServiceMeta ds2 = factory.createDataService( stepMeta );
    assertTrue( TransientResolver.isTransient( ds2.getName() ) );

    RepositoryDirectoryInterface repositoryDirectoryInterface = mock( RepositoryDirectoryInterface.class );
    when( repositoryDirectoryInterface.getPath() ).thenReturn( "/repository/path" );
    when( transMeta.getObjectId() ).thenReturn( mock( ObjectId.class ) );
    when( transMeta.getFilename() ).thenReturn( "/unix/path/file.ktr" );
    when( transMeta.getRepositoryDirectory() ).thenReturn( repositoryDirectoryInterface );
    DataServiceMeta ds3 = factory.createDataService( stepMeta );
    assertTrue( TransientResolver.isTransient( ds3.getName() ) );

    repositoryDirectoryInterface = mock( RepositoryDirectoryInterface.class );
    when( repositoryDirectoryInterface.getPath() ).thenReturn( "/repository/path" );
    when( transMeta.getFilename() ).thenReturn( "" );
    when( transMeta.getRepositoryDirectory() ).thenReturn( repositoryDirectoryInterface );
    DataServiceMeta ds6 = factory.createDataService( stepMeta );
    assertTrue( TransientResolver.isTransient( ds6.getName() ) );

    when( repositoryDirectoryInterface.getPath() ).thenReturn( "/repository/path/" );
    when( transMeta.getFilename() ).thenReturn( "" );
    when( transMeta.getRepositoryDirectory() ).thenReturn( repositoryDirectoryInterface );
    DataServiceMeta ds4 = factory.createDataService( stepMeta );
    assertTrue( TransientResolver.isTransient( ds4.getName() ) );

    when( transMeta.getRepositoryDirectory() ).thenReturn( null );
    DataServiceMeta ds5 = factory.createDataService( stepMeta );
    assertTrue( TransientResolver.isTransient( ds5.getName() ) );
  }
}
