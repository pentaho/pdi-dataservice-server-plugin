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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryElementMetaInterface;
import org.pentaho.di.repository.RepositoryObject;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.TransMeta;

import java.net.URL;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class ServiceTransTest {
  @Mock TransMeta transMeta;
  @Mock Repository repository;

  @Test
  public void testRepoIdStorageMethod() throws KettleException {
    ObjectId transId = new StringObjectId( "transId" );
    when( repository.loadTransformation( eq( transId ), isNull( String.class ) ) ).thenReturn( transMeta );

    ServiceTrans.StorageMethod method = ServiceTrans.StorageMethod.REPO_ID;
    when( repository.getObjectInformation( any( ObjectId.class ), eq( RepositoryObjectType.TRANSFORMATION ) ) )
        .thenReturn( mock( RepositoryObject.class ) ).thenThrow( new KettleException() );
    assertThat( method.exists( repository, transId.getId() ), is( true ) );
    assertThat( method.exists( repository, "/nonExistingTrans" ), is( false ) );

    RepositoryObject transInfo = mock( RepositoryObject.class );
    when( transInfo.isDeleted() ).thenReturn( false, true );
    when( repository.getObjectInformation( eq( transId ),
        eq( RepositoryObjectType.TRANSFORMATION ) ) ).thenReturn( transInfo );
    assertThat( method.load( repository, transId.getId() ), is( transMeta ) );
    assertThat( method.load( repository, transId.getId() ), is( nullValue() ) );
  }

  @Test
  public void testRepoPathStorageMethod() throws KettleException {
    ObjectId transId = new StringObjectId( "transId" );
    when( repository.loadTransformation( eq( transId ), isNull( String.class ) ) ).thenReturn( transMeta );

    RepositoryDirectoryInterface root = mock( RepositoryDirectoryInterface.class );
    ObjectId rootId = new StringObjectId( "dirId" );
    when( root.getObjectId() ).thenReturn( rootId );
    when( repository.loadRepositoryDirectoryTree() ).thenReturn( root );
    RepositoryDirectoryInterface dir = mock( RepositoryDirectoryInterface.class );

    lenient().when( root.findDirectory( eq( "/existing" ) ) ).thenReturn( dir, null );
    lenient().when( repository.getTransformationID( eq( "Trans" ), eq( dir ) ) ).thenReturn( null );
    RepositoryElementMetaInterface transInfo = mock( RepositoryElementMetaInterface.class );
    when( transInfo.getName() ).thenReturn( "Trans" );
    when( transInfo.getObjectId() ).thenReturn( transId );
    lenient().when( repository.getTransformationObjects( eq( rootId ), eq( true ) ) ).thenReturn( Arrays.asList( transInfo ) );

    ServiceTrans.StorageMethod method = ServiceTrans.StorageMethod.REPO_PATH;

    assertThat( method.exists( repository, "/existing/Trans" ), is( true ) );

    RepositoryObject transObjectInfo = mock( RepositoryObject.class );
    when( transObjectInfo.isDeleted() ).thenReturn( false, true );

    when( repository.getObjectInformation( eq( transId ), eq( RepositoryObjectType.TRANSFORMATION ) ) )
        .thenReturn( transObjectInfo );

    assertThat( method.load( repository, "/existing/Trans" ), is( transMeta ) );
    assertThat( method.load( repository, "/existing/Trans" ), is( nullValue() ) );

  }

  @Test
  public void testLoadDeletedTransFileStorageMethod() throws Exception {
    ServiceTrans.StorageMethod method = ServiceTrans.StorageMethod.FILE;
    TransMeta nonExistingTrans = method.load( null, "fakePath" );
    assertThat( nonExistingTrans, is( nullValue() ) );
  }

  @Test
  public void testLoadFileStorageMethod() throws Exception {
    ServiceTrans.StorageMethod method = ServiceTrans.StorageMethod.FILE;
    URL resource = getClass().getClassLoader().getResource( "showAnnotations.ktr" );
    TransMeta transMeta = method.load( null, resource.getPath() );
    assertThat( transMeta, is( notNullValue() ) );
  }
}
