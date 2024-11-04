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


package org.pentaho.di.trans.dataservice.serialization;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.attributes.metastore.EmbeddedMetaStore;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryCapabilities;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.Context;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class DataServiceReferenceSynchronizerTest {

  @Mock TransMeta transMeta;
  @Mock Context context;
  @Mock EmbeddedMetaStore embeddedMetastore;
  @Mock IMetaStore externalMetastore;
  @Mock MetaStoreFactory embeddedMetaStoreFactory;
  @Mock MetaStoreFactory externalMetaStoreFactory;
  @Mock DataServiceMeta dataServiceMeta;
  @Mock Repository repository;
  @Mock RepositoryMeta repositoryMeta;
  @Mock Function exceptionHandler;
  @Mock MetaStoreException exception;

  @Captor ArgumentCaptor<ServiceTrans> captor;

  DataServiceReferenceSynchronizer synchronizer;

  @Before
  public void before() {
    ObjectId transId = new StringObjectId( "transId" );
    when( transMeta.getMetaStore() ).thenReturn( externalMetastore );
    when( transMeta.getEmbeddedMetaStore() ).thenReturn( embeddedMetastore );
    when( transMeta.getRepository() ).thenReturn( repository );

    when( repository.getRepositoryMeta() ).thenReturn( repositoryMeta );
    when( repositoryMeta.getRepositoryCapabilities() ).thenReturn( mock( RepositoryCapabilities.class ) );
    RepositoryDirectoryInterface root = mock( RepositoryDirectoryInterface.class );
    RepositoryDirectoryInterface dir = new RepositoryDirectory( root, "location" );

    when( root.findDirectory( eq( "/" ) ) ).thenReturn( dir );
    try {
      when( repository.getTransformationID( eq( "location" ), eq( dir ) ) ).thenReturn( transId );
      when( repository.loadRepositoryDirectoryTree() ).thenReturn( root );
    } catch ( KettleException ke ) {
      // Noop
    }


    when( context.getPushDownFactories() ).thenReturn( Collections.emptyList() );
    synchronizer = new DataServiceReferenceSynchronizer( context ) {
      protected <T> MetaStoreFactory<T> getMetastoreFactory( IMetaStore metaStore, Class<T> clazz ) {
        return externalMetaStoreFactory;
      }
      protected MetaStoreFactory<DataServiceMeta> getEmbeddedMetaStoreFactory( TransMeta meta ) {
        return embeddedMetaStoreFactory;
      }
    };
  }

  @Test
  public void testSyncWithNothingPreviouslyPublished() throws MetaStoreException {
    List<DataServiceMeta> metas = mockDataServiceMetas( "foo", "bar" );
    when( embeddedMetaStoreFactory.getElements() ).thenReturn( metas );
    when( externalMetaStoreFactory.getElements() ).thenReturn( Collections.emptyList() );
    synchronizer.sync( transMeta, exceptionHandler );

    verify( externalMetaStoreFactory, times( 2 ) ).saveElement( captor.capture() );

    assertThat( captor.getAllValues().stream()
      .map( svcTrans -> svcTrans.getName() ).collect( Collectors.toList() ),
      containsInAnyOrder( "foo", "bar" ) );
    verify( externalMetaStoreFactory, times( 0 ) ).deleteElement( anyString() );
  }

  @Test
  public void testSyncWithOtherStuffPublished() throws MetaStoreException {
    when( transMeta.getPathAndName() ).thenReturn( "/transLocation" );

    List<DataServiceMeta> metas = mockDataServiceMetas( "foo", "bar", "baz" );
    List<ServiceTrans> serviceTrans = mockDataServiceTrans( "/location", "bop", "bip", "fip" );
    setupMetaStoreFactoryMocks( metas, serviceTrans );
    synchronizer.sync( transMeta, null, true );

    verify( externalMetaStoreFactory, times( 3 ) ).saveElement( captor.capture() );
    assertThat( captor.getAllValues().stream()
        .map( svcTrans -> svcTrans.getName() ).collect( Collectors.toList() ),
      containsInAnyOrder( "foo", "bar", "baz" ) );
    verify( externalMetaStoreFactory, times( 0 ) ).deleteElement( anyString() );
  }

  @Test
  public void testSyncWithConflictsNoOverwrite() throws MetaStoreException {
    when( transMeta.getPathAndName() ).thenReturn( "/transLocation" );

    List<DataServiceMeta> metas = mockDataServiceMetas( "foo", "bar", "baz" );
    List<ServiceTrans> serviceTrans = mockDataServiceTrans( "/location", "bar", "bip", "fip" );
    setupMetaStoreFactoryMocks( metas, serviceTrans );
    synchronizer.sync( transMeta, exceptionHandler, false );

    verify( externalMetaStoreFactory, times( 2 ) ).saveElement( captor.capture() );
    assertThat( captor.getAllValues().stream()
        .map( svcTrans -> svcTrans.getName() ).collect( Collectors.toList() ),
      containsInAnyOrder( "foo", "baz" ) );
    verify( externalMetaStoreFactory, times( 0 ) ).deleteElement( anyString() );
  }

  @Test
  public void testSyncWithConflictsWithOverwrite() throws MetaStoreException {
    when( transMeta.getPathAndName() ).thenReturn( "/transLocation" );

    List<DataServiceMeta> metas = mockDataServiceMetas( "foo", "bar", "baz" );
    List<ServiceTrans> serviceTrans = mockDataServiceTrans( "/location", "bar", "bip", "fip" );
    setupMetaStoreFactoryMocks( metas, serviceTrans );
    synchronizer.sync( transMeta, exceptionHandler, true );

    verify( externalMetaStoreFactory, times( 3 ) ).saveElement( captor.capture() );
    assertThat( captor.getAllValues().stream()
        .map( svcTrans -> svcTrans.getName() ).collect( Collectors.toList() ),
      containsInAnyOrder( "foo", "bar", "baz" ) );
    verify( externalMetaStoreFactory, times( 0 ) ).deleteElement( anyString() );
  }

  @Test
  public void testCleanupUnusedReferences() throws MetaStoreException {
    when( transMeta.getPathAndName() ).thenReturn( "/location" );

    List<DataServiceMeta> metas = mockDataServiceMetas( "foo", "bar", "baz" );
    List<ServiceTrans> serviceTrans = mockDataServiceTrans( "/location", "bar", "bip", "fip" );
    setupMetaStoreFactoryMocks( metas, serviceTrans );
    synchronizer.sync( transMeta, exceptionHandler, true );
    verify( externalMetaStoreFactory, times( 3 ) ).saveElement( captor.capture() );
    assertThat( captor.getAllValues().stream()
        .map( svcTrans -> svcTrans.getName() ).collect( Collectors.toList() ),
      containsInAnyOrder( "foo", "bar", "baz" ) );
    verify( externalMetaStoreFactory, times( 1 ) ).deleteElement( "bip" );
    verify( externalMetaStoreFactory, times( 1 ) ).deleteElement( "fip" );
  }

  @Test
  public void testMetastoreExceptionDuringSvcLoad() throws MetaStoreException {
    when( embeddedMetaStoreFactory.getElements() ).thenThrow( exception );
    synchronizer.sync( transMeta, exceptionHandler, true );
    verify( exceptionHandler, times( 1 ) ).apply( exception );
  }

  @Test
  public void testMetastoreExceptionDuringSave() throws MetaStoreException {
    doThrow( exception ).when( externalMetaStoreFactory ).saveElement( any() );
    List<DataServiceMeta> metas = mockDataServiceMetas( "foo" );
    List<ServiceTrans> serviceTrans = mockDataServiceTrans( "/location", "bar" );
    setupMetaStoreFactoryMocks( metas, serviceTrans );
    synchronizer.sync( transMeta, exceptionHandler, true );
    verify( exceptionHandler, times( 1 ) ).apply( exception );
  }

  @Test
  public void testMetastoreExceptionDuringDelete() throws MetaStoreException {
    when( transMeta.getPathAndName() ).thenReturn( "/location" );
    doThrow( exception ).when( externalMetaStoreFactory ).deleteElement( any() );
    List<DataServiceMeta> metas = mockDataServiceMetas( "foo" );
    List<ServiceTrans> serviceTrans = mockDataServiceTrans( "/location", "bar" );
    setupMetaStoreFactoryMocks( metas, serviceTrans );
    synchronizer.sync( transMeta, exceptionHandler, true );
    verify( exceptionHandler, times( 1 ) ).apply( exception );
  }

  @Test
  public void testGetMetastoreFactories() throws MetaStoreException {
    DataServiceReferenceSynchronizer sync = new DataServiceReferenceSynchronizer( context );
    assertThat( sync.getEmbeddedMetaStoreFactory( transMeta ), instanceOf( EmbeddedMetaStoreFactory.class ) );
    assertThat( sync.getMetastoreFactory( externalMetastore, ServiceTrans.class ).getMetaStore(),
      is( externalMetastore ) );

  }

  private void setupMetaStoreFactoryMocks( List<DataServiceMeta> metas, List<ServiceTrans> serviceTrans )
    throws MetaStoreException {
    when( embeddedMetaStoreFactory.getElements() ).thenReturn( metas );
    when( externalMetaStoreFactory.getElements() ).thenReturn( serviceTrans );
  }


  private List<DataServiceMeta> mockDataServiceMetas( String... names ) {
    List<DataServiceMeta> metas = new ArrayList<>();
    for ( String name : names ) {
      DataServiceMeta meta = mock( DataServiceMeta.class );
      when( meta.getName() ).thenReturn( name );
      when( meta.getServiceTrans() ).thenReturn( transMeta );
      metas.add( meta );
    }
    return metas;
  }

  private List<ServiceTrans> mockDataServiceTrans( String location, String... names ) {
    List<ServiceTrans> serviceTranses = new ArrayList<>();
    for ( String name : names ) {
      ServiceTrans serviceTrans = mock( ServiceTrans.class );
      when( serviceTrans.getName() ).thenReturn( name );
      ServiceTrans.Reference ref = new ServiceTrans.Reference( ServiceTrans.StorageMethod.REPO_PATH, location );
      when( serviceTrans.getReferences() ).thenReturn( Arrays.asList( ref ) );
      serviceTranses.add( serviceTrans );
    }
    return serviceTranses;
  }
}
