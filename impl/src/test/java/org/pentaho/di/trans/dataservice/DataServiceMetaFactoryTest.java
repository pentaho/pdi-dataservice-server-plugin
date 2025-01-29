/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.dataservice;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
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

@RunWith( MockitoJUnitRunner.StrictStubs.class)
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

    when( transMeta.getName() ).thenReturn( transName );
    when( cacheFactory.createPushDown() ).thenReturn( new ServiceCache( cacheFactory ) );

    when( stepMeta.getName() ).thenReturn( stepName );
    when( stepMeta.getParentTransMeta() ).thenReturn( transMeta );
    when( spoon.getActiveTransformation() ).thenReturn( activeTransMeta );
  }

  @Test
  public void testCreateDataServiceSpoon() throws Exception {
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
    when( spoon.getActiveTransformation() ).thenReturn( transMeta );
    testCreateDataServiceCommon();
  }

  private void testCreateDataServiceCommon() throws Exception {
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
