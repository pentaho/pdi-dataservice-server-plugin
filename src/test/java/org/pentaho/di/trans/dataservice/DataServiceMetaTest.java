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

import org.junit.Test;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryCapabilities;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.trans.TransMeta;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DataServiceMetaTest {

  @Test
  public void testCreateCacheKey() throws Exception {
    String stepName = "Test Step";
    String id = "1234567890";

    TransMeta transMeta = mock( TransMeta.class );
    Repository repository = mock( Repository.class );
    RepositoryMeta repositoryMeta = mock( RepositoryMeta.class );
    RepositoryCapabilities repositoryCapabilities = mock( RepositoryCapabilities.class );
    ObjectId objectId = mock( ObjectId.class );

    doReturn( repository ).when( transMeta ).getRepository();
    doReturn( repositoryMeta ).when( repository ).getRepositoryMeta();
    doReturn( repositoryCapabilities ).when( repositoryMeta ).getRepositoryCapabilities();
    doReturn( true ).when( repositoryCapabilities ).supportsReferences();

    doReturn( objectId ).when( transMeta ).getObjectId();
    doReturn( id ).when( objectId ).getId();

    DataServiceMeta.CacheKey cacheKey = DataServiceMeta.createCacheKey( transMeta, stepName );
    DataServiceMeta.CacheKey equivalentCacheKey = new DataServiceMeta.CacheKey( id, stepName );

    assertEquals( cacheKey, equivalentCacheKey );
    assertEquals( cacheKey.hashCode(), equivalentCacheKey.hashCode() );

    // Test for file in repo with no object ID
    id = "/path/in/repo/file.ktr";

    doReturn( false ).when( repositoryCapabilities ).supportsReferences();
    doReturn( id ).when( transMeta ).getPathAndName();

    cacheKey = DataServiceMeta.createCacheKey( transMeta, stepName );
    equivalentCacheKey = new DataServiceMeta.CacheKey( id, stepName );

    assertEquals( cacheKey, equivalentCacheKey );
    assertEquals( cacheKey.hashCode(), equivalentCacheKey.hashCode() );

    // Test for file not in repo
    id = "/path/in/system/file.ktr";

    doReturn( null ).when( transMeta ).getRepository();
    doReturn( id ).when( transMeta ).getFilename();

    cacheKey = DataServiceMeta.createCacheKey( transMeta, stepName );
    equivalentCacheKey = new DataServiceMeta.CacheKey( id, stepName );

    assertEquals( cacheKey, equivalentCacheKey );
    assertEquals( cacheKey.hashCode(), equivalentCacheKey.hashCode() );

    verify( transMeta, times( 3 ) ).getRepository();
    verify( repository, times( 2 ) ).getRepositoryMeta();
    verify( repositoryMeta, times( 2 ) ).getRepositoryCapabilities();
    verify( repositoryCapabilities, times( 2 ) ).supportsReferences();
    verify( transMeta ).getObjectId();
    verify( objectId ).getId();
    verify( transMeta ).getPathAndName();
    verify( transMeta ).getFilename();
  }
}
