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
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.ui.repository.RepositoryExtension;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryDirectories;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryDirectory;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryObject;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryObjects;

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bmorrise on 10/26/15.
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class DeleteRepositoryObjectExtensionPointPluginTest {

  @Mock
  DataServiceMetaStoreUtil metaStoreUtil;

  @Mock
  DataServiceContext context;

  @Mock
  LogChannelInterface log;

  @Mock
  RepositoryExtension repositoryExtension;

  @Mock
  UIRepositoryObject repositoryObject;

  @Mock
  UIRepositoryDirectory repositoryEmptyDirectory;

  @Mock
  UIRepositoryDirectory repositoryParentDirectory;

  @Mock
  UIRepositoryDirectory repositoryParentWSubFolderDirectory;

  @Mock
  UIRepositoryDirectory repositorySubDirectory;

  @Mock
  Repository repository;

  @Mock
  ObjectId objectId;

  @Mock
  TransMeta transMeta;

  @Mock
  UIRepositoryObjects repoObjects;

  @Mock
  UIRepositoryDirectories repoDirectories;

  DeleteRepositoryObjectExtensionPointPlugin plugin;

  @Before
  public void setUp() throws Exception {
    when( context.getMetaStoreUtil() ).thenReturn( metaStoreUtil );
    plugin = new DeleteRepositoryObjectExtensionPointPlugin( context );
    repoObjects = new UIRepositoryObjects( asList( repositoryObject ) );
    repoDirectories = new UIRepositoryDirectories();
    repoDirectories.add( repositorySubDirectory );
  }

  @Test
  public void testCallExtensionPoint() throws Exception {
    when( repositoryExtension.getRepositoryObject() ).thenReturn( repositoryObject );
    when( repositoryObject.getRepositoryElementType() ).thenReturn( RepositoryObjectType.TRANSFORMATION );
    when( repositoryObject.getRepository() ).thenReturn( repository );
    when( repositoryObject.getObjectId() ).thenReturn( objectId );
    when( repository.loadTransformation( objectId, null ) ).thenReturn( transMeta );

    plugin.callExtensionPoint( log, repositoryExtension );

    verify( repositoryExtension ).getRepositoryObject();
    verify( repositoryObject ).getRepositoryElementType();
    verify( repositoryObject ).getRepository();
    verify( metaStoreUtil ).clearReferences( any( TransMeta.class ) );
  }

  @Test
  public void testCallExtensionPointWEmptyDirectory() throws Exception {
    when( repositoryExtension.getRepositoryObject() ).thenReturn( repositoryEmptyDirectory );
    when( repositoryEmptyDirectory.getChildren() ).thenReturn( null );
    when( repositoryEmptyDirectory.getRepositoryObjects() ).thenReturn( new UIRepositoryObjects() );

    plugin.callExtensionPoint( log, repositoryExtension );

    verify( repositoryExtension ).getRepositoryObject();
    verify( repositoryEmptyDirectory ).getChildren();
    verify( repositoryEmptyDirectory ).getChildren();
    verify( repositoryEmptyDirectory ).getRepositoryObjects();
    verify( metaStoreUtil, never() ).clearReferences( any( TransMeta.class ) );
  }

  @Test
  public void testCallExtensionPointWParentDirectory() throws Exception {
    when( repositoryExtension.getRepositoryObject() ).thenReturn( repositoryParentDirectory );
    when( repositoryParentDirectory.getChildren() ).thenReturn( null );
    when( repositoryParentDirectory.getRepositoryObjects() ).thenReturn( repoObjects );
    when( repositoryObject.getRepositoryElementType() ).thenReturn( RepositoryObjectType.TRANSFORMATION );
    when( repositoryObject.getRepository() ).thenReturn( repository );
    when( repositoryObject.getObjectId() ).thenReturn( objectId );
    when( repository.loadTransformation( objectId, null ) ).thenReturn( transMeta );

    plugin.callExtensionPoint( log, repositoryExtension );

    verify( repositoryExtension ).getRepositoryObject();
    verify( repositoryParentDirectory ).getChildren();
    verify( repositoryParentDirectory ).getRepositoryObjects();
    verify( repositoryObject ).getRepositoryElementType();
    verify( repositoryObject ).getRepository();
    verify( metaStoreUtil ).clearReferences( any( TransMeta.class ) );
  }

  @Test
  public void testCallExtensionPointSubDirectory() throws Exception {
    when( repositoryExtension.getRepositoryObject() ).thenReturn( repositoryParentDirectory );
    when( repositoryParentDirectory.getChildren() ).thenReturn( repoDirectories );
    when( repositoryParentDirectory.getRepositoryObjects() ).thenReturn( repoObjects );
    when( repositorySubDirectory.getChildren() ).thenReturn( null );
    when( repositorySubDirectory.getRepositoryObjects() ).thenReturn( repoObjects );
    when( repositoryObject.getRepositoryElementType() ).thenReturn( RepositoryObjectType.TRANSFORMATION );
    when( repositoryObject.getRepository() ).thenReturn( repository );
    when( repositoryObject.getObjectId() ).thenReturn( objectId );
    when( repository.loadTransformation( objectId, null ) ).thenReturn( transMeta );

    plugin.callExtensionPoint( log, repositoryExtension );

    verify( repositoryExtension ).getRepositoryObject();
    verify( repositoryParentDirectory, times( 3 ) ).getChildren();
    verify( repositoryParentDirectory ).getRepositoryObjects();
    verify( repositorySubDirectory, times( 1 ) ).getChildren();
    verify( repositorySubDirectory ).getRepositoryObjects();
    verify( repositoryObject, times( 2 ) ).getRepositoryElementType();
    verify( repositoryObject, times( 2 ) ).getRepository();
    verify( metaStoreUtil, times( 2 ) ).clearReferences( any( TransMeta.class ) );
  }

}
