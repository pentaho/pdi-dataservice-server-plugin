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


package org.pentaho.di.trans.dataservice.resolvers;

import com.google.common.io.Files;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.cache.ServiceCache;
import org.pentaho.di.trans.dataservice.optimization.cache.ServiceCacheFactory;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.dummytrans.DummyTransMeta;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.kettle.repository.locator.api.KettleRepositoryLocator;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bmorrise on 9/13/16.
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class TransientResolverTest {

  private TransientResolver transientResolver;

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock private KettleRepositoryLocator kettleRepositoryLocator;
  @Mock private DataServiceContext context;
  @Mock private ServiceCacheFactory serviceCacheFactory;
  @Mock private ServiceCache serviceCache;
  @Mock private Repository repository;
  @Mock private RepositoryDirectoryInterface root;
  @Mock private ObjectId objectId;
  @Mock private Spoon spoon;
  @Mock private TransMeta activeTransMeta;
  @Mock private com.google.common.base.Function<Exception, Void> logger;

  private TransMeta transMeta;

  @BeforeClass
  public static void setUpClass() throws Exception {
    if ( !KettleClientEnvironment.isInitialized() ) {
      KettleClientEnvironment.init();
    }
  }

  @Before
  public void setup() throws Exception {
    when( kettleRepositoryLocator.getRepository() ).thenReturn( repository );
    when( repository.loadRepositoryDirectoryTree() ).thenReturn( root );
    when( serviceCacheFactory.createPushDown() ).thenReturn( serviceCache );
    when( spoon.getActiveTransformation() ).thenReturn( activeTransMeta );
    when( activeTransMeta.realClone( false ) ).thenReturn( activeTransMeta );

    transMeta = new TransMeta();
    StepMeta output = new StepMeta();
    output.setName( "OUTPUT" );
    output.setStepMetaInterface( new DummyTransMeta() );
    transMeta.addStep( output );

    transientResolver = new TransientResolver( kettleRepositoryLocator, context, serviceCacheFactory, LogLevel.DEBUG,
      () -> spoon );
  }

  @Test
  public void testGetDataServiceWithFileSeparator() throws Exception {
    testGetDataService( File.separator );
  }

  @Test
  public void testGetDataServiceWithForwardSlash() throws Exception {
    testGetDataService( "/" );
  }

  @Test
  public void testGetDataServiceWithBackSlash() throws Exception {
    testGetDataService( "\\" );
  }

  private void testGetDataService( String fileSeparator ) throws Exception {
    RepositoryDirectoryInterface directory = mock( RepositoryDirectoryInterface.class );

    when( root.findDirectory( fileSeparator + "path" + fileSeparator + "to" ) ).thenReturn( directory );
    when( repository.getTransformationID( "name", directory ) ).thenReturn( objectId );
    when( repository.loadTransformation( objectId, null ) ).thenReturn( transMeta );
    String transientId = TransientResolver.buildTransient( fileSeparator + "path" + fileSeparator + "to" + fileSeparator + "name", "data_service" );

    DataServiceMeta dataServiceMeta = transientResolver.getDataService( transientId );

    assertNotNull( dataServiceMeta );
    assertThat( dataServiceMeta.getStepname(), is( "data_service" ) );
    assertFalse( dataServiceMeta.isUserDefined() );
    assertThat( dataServiceMeta, hasServiceCacheOptimization() );

    int rowLimit = 1000;
    transientId = TransientResolver.buildTransient( fileSeparator + "path" + fileSeparator + "to" + fileSeparator + "name", "data_service", rowLimit );
    dataServiceMeta = transientResolver.getDataService( transientId );
    assertEquals( rowLimit, dataServiceMeta.getRowLimit() );
  }

  @Test
  public void testGetDataServiceDisconnected() throws Exception {
    when( kettleRepositoryLocator.getRepository() ).thenReturn( null );
    File localFolder = temporaryFolder.newFolder( "local" );
    File localFile = new File( localFolder, "name.ktr" );
    Files.write( transMeta.getXML().getBytes( StandardCharsets.UTF_8 ), localFile );
    String transientId = TransientResolver.buildTransient( localFile.getPath(), "data_service" );

    DataServiceMeta dataServiceMeta = transientResolver.getDataService( transientId );

    assertNotNull( dataServiceMeta );
    assertThat( dataServiceMeta.getStepname(), is( "data_service" ) );
    assertFalse( dataServiceMeta.isUserDefined() );
    assertThat( dataServiceMeta, hasServiceCacheOptimization() );
  }

  @Test
  public void testGetLocalDataService() throws Exception {
    File localFolder = temporaryFolder.newFolder( "local" );
    File localFile = new File( localFolder, "name.ktr" );
    Files.write( transMeta.getXML().getBytes( StandardCharsets.UTF_8 ), localFile );
    String transientId = TransientResolver.buildTransient( localFile.getPath(), "data_service" );

    when( repository.getTransformationID( any(), eq( root ) ) ).thenThrow( new KettleException() );

    DataServiceMeta dataServiceMeta = transientResolver.getDataService( transientId );

    assertNotNull( dataServiceMeta );
    assertThat( dataServiceMeta.getStepname(), is( "data_service" ) );
    assertFalse( dataServiceMeta.isUserDefined() );
    assertThat( dataServiceMeta, hasServiceCacheOptimization() );
  }

  @Test
  public void testBuilderPassesLogLevel() throws Exception {
    File localFolder = temporaryFolder.newFolder( "local" );
    File localFile = new File( localFolder, "name.ktr" );
    Files.write( transMeta.getXML().getBytes( StandardCharsets.UTF_8 ), localFile );
    String transientId = TransientResolver.buildTransient( localFile.getPath(), "OUTPUT" );

    when( repository.getTransformationID( any(), eq( root ) ) ).thenThrow( new KettleException() );

    DataServiceExecutor.Builder builder =
      transientResolver.createBuilder( new SQL( "select * from " + transientId ) );
    DataServiceExecutor build = builder.build();
    assertEquals( LogLevel.DEBUG, build.getServiceTransMeta().getLogLevel() );
  }

  @Test
  public void testBuildLocalTransient() throws Exception {
    String transientId = TransientResolver.buildTransient( "/path/to/file.ktr", "local:OUTPUT" );
    String[] parts = transientResolver.splitTransient( transientId );
    assertEquals( parts.length, 2 );

    DataServiceMeta dataServiceMeta = transientResolver.getDataService( transientId );
    assertEquals( dataServiceMeta.getStepname(), "OUTPUT" );
  }

  @Test
  public void testBuildStreamingTransient() throws Exception {
    RepositoryDirectoryInterface directory = mock( RepositoryDirectoryInterface.class );

    when( root.findDirectory( "\\path\\to" ) ).thenReturn( directory );
    when( repository.getTransformationID( "name", directory ) ).thenReturn( objectId );
    when( repository.loadTransformation( objectId, null ) ).thenReturn( transMeta );

    String transientId = TransientResolver.buildTransient( "\\path\\to\\name", "streaming:data_service" );
    String[] parts = transientResolver.splitTransient( transientId );
    assertEquals( parts.length, 2 );

    DataServiceMeta dataServiceMeta = transientResolver.getDataService( transientId );
    assertEquals( dataServiceMeta.getStepname(), "data_service" );
  }

  @Test
  public void testBuildLocalStreamingTransient() throws Exception {
    String transientId = TransientResolver.buildTransient( "/path/to/file.ktr",
      "local:streaming:OUTPUT" );
    String[] parts = transientResolver.splitTransient( transientId );
    assertEquals( parts.length, 2 );

    DataServiceMeta dataServiceMeta = transientResolver.getDataService( transientId );
    assertEquals( dataServiceMeta.getStepname(), "OUTPUT" );
  }

  @Test
  public void testBuildStreamingLocalTransient() throws Exception {
    String transientId = TransientResolver.buildTransient( "/path/to/file.ktr",
      "streaming:local:OUTPUT" );
    String[] parts = transientResolver.splitTransient( transientId );
    assertEquals( parts.length, 2 );

    DataServiceMeta dataServiceMeta = transientResolver.getDataService( transientId );
    assertEquals( dataServiceMeta.getStepname(), "OUTPUT" );
  }

  @Test
  public void testGetDataServicesNames() {
    assertEquals( transientResolver.getDataServiceNames(), new ArrayList<>() );
  }

  @Test
  public void testGetDataServices() {
    assertEquals( transientResolver.getDataServices( logger ), new ArrayList<>() );
  }

  private Matcher<DataServiceMeta> hasServiceCacheOptimization() {
    return new TypeSafeMatcher<DataServiceMeta>() {
      @Override protected boolean matchesSafely( DataServiceMeta item ) {
        return item.getPushDownOptimizationMeta().stream()
          .map( PushDownOptimizationMeta::getType )
          .filter( Predicate.isEqual( serviceCache ) )
          .findAny().isPresent();
      }

      @Override public void describeTo( Description description ) {
        description.appendText( "has service cache optimization" );
      }
    };
  }
}
