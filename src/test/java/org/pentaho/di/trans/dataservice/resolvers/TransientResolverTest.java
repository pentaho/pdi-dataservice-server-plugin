/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.resolvers;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.cache.ServiceCacheFactory;
import org.pentaho.osgi.kettle.repository.locator.api.KettleRepositoryLocator;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bmorrise on 9/13/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class TransientResolverTest {

  private final String DATA_SERVICE_NAME = "transient:L3BhdGgvdG8vZmlsZQ==:U3RlcCBOYW1l";

  private TransientResolver transientResolver;

  @Mock private KettleRepositoryLocator kettleRepositoryLocator;
  @Mock private DataServiceContext context;
  @Mock private ServiceCacheFactory serviceCacheFactory;
  @Mock private Repository repository;
  @Mock private RepositoryDirectoryInterface repositoryDirectoryInterface;
  @Mock private ObjectId objectId;
  @Mock private TransMeta transMeta;

  @Before
  public void setup() throws Exception {
    when( kettleRepositoryLocator.getRepository() ).thenReturn( repository );
    when( repository.loadRepositoryDirectoryTree() ).thenReturn( repositoryDirectoryInterface );
    when( repositoryDirectoryInterface.findDirectory( "/path/to/" ) ).thenReturn( null );
    when( repository.getTransformationID( "name.ktr", repositoryDirectoryInterface ) ).thenReturn( objectId );
    when( repository.loadTransformation( objectId, null ) ). thenReturn( transMeta );
    when( serviceCacheFactory.createPushDown() ).thenReturn( null );

    transientResolver = new TransientResolver( kettleRepositoryLocator, context, serviceCacheFactory );
  }

  @Test
  public void testGetDataService() {
    DataServiceMeta dataServiceMeta = transientResolver.getDataService( DATA_SERVICE_NAME );

    assertNotNull( dataServiceMeta );

    verify( kettleRepositoryLocator ).getRepository();
    verify( serviceCacheFactory ).createPushDown();
  }
}
