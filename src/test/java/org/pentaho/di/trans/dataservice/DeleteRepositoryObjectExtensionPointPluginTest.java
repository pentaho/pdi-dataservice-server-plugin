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

package org.pentaho.di.trans.dataservice;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.ui.repository.RepositoryExtension;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryObject;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bmorrise on 10/26/15.
 */
@RunWith( MockitoJUnitRunner.class )
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
  Repository repository;

  @Mock
  ObjectId objectId;

  @Mock
  TransMeta transMeta;

  DeleteRepositoryObjectExtensionPointPlugin plugin;

  @Before
  public void setUp() throws Exception {
    when( context.getMetaStoreUtil() ).thenReturn( metaStoreUtil );
    plugin = new DeleteRepositoryObjectExtensionPointPlugin( context );
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


}
