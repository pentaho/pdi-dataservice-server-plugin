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

import org.pentaho.di.trans.dataservice.cache.DataServiceMetaCache;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.junit.Test;
import org.pentaho.caching.api.PentahoCacheManager;
import org.pentaho.di.core.sql.ServiceCacheMethod;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryCapabilities;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

public class DataServiceMetaTest {

  @Test
  public void testDataServiceMetaSerialization() throws MetaStoreException {
    IMetaStore metaStore = new MemoryMetaStore();
    DataServiceMeta[] dataServiceMetas = new DataServiceMeta[] {
      makeTestDSM( "name", "stepName", "/my/transFilename.ktr", "transRepPath", null, ServiceCacheMethod.None, 5 ),
      makeTestDSM( "name2", "stepName 2", "/foo/bar/baz.ktr", "transRepPath", "otherOid", ServiceCacheMethod.None, 15 ),
      makeTestDSM( "name 3", "stepName3", null, "transRepPath", "blahOid", ServiceCacheMethod.LocalMemory, 0 ),
    };
    DataServiceMetaStoreUtil metaStoreUtil = new DataServiceMetaStoreUtil( Collections.<PushDownFactory>emptyList(), mock(
      DataServiceMetaCache.class ) );
    MetaStoreFactory<DataServiceMeta> factory = metaStoreUtil.getMetaStoreFactory( metaStore );

    for ( DataServiceMeta meta : dataServiceMetas ) {
      factory.saveElement( meta );
    }
    for ( DataServiceMeta meta : dataServiceMetas ) {
      DataServiceMeta returnedDSM = factory.loadElement( meta.getName() );
      assertEquals( meta.getStepname(), returnedDSM.getStepname() );
      assertEquals( meta.getTransFilename(), returnedDSM.getTransFilename() );
      assertEquals( meta.getTransRepositoryPath(), returnedDSM.getTransRepositoryPath() );
      assertEquals( meta.getTransObjectId(), returnedDSM.getTransObjectId() );
      assertEquals( meta.getCacheMethod(), returnedDSM.getCacheMethod() );
      assertEquals( meta.getCacheMaxAgeMinutes(), returnedDSM.getCacheMaxAgeMinutes() );
    }
  }

  private DataServiceMeta makeTestDSM( String name, String stepName, String transFilename,
                                       String transRepPath, String transOid,
                                       ServiceCacheMethod cacheMethod, int cacheAgeMinutes ) {
    DataServiceMeta dsm = new DataServiceMeta();
    dsm.setName( name );
    dsm.setStepname( stepName );
    dsm.setTransFilename( transFilename );
    dsm.setTransRepositoryPath( transRepPath );
    dsm.setTransObjectId( transOid );
    dsm.setCacheMethod( cacheMethod );
    dsm.setCacheMaxAgeMinutes( cacheAgeMinutes );
    return dsm;
  }

  @Test
  public void testLookupTransObjectId() throws Exception {
    Repository repository = mock( Repository.class );
    RepositoryDirectoryInterface rootDir = mock( RepositoryDirectoryInterface.class );
    RepositoryDirectoryInterface dataServiceDir = mock( RepositoryDirectoryInterface.class );
    String repDir = "/public/dataServices/", transName = "myService.ktr";
    StringObjectId objectId = new StringObjectId( UUID.randomUUID().toString() );

    when( repository.loadRepositoryDirectoryTree() ).thenReturn( rootDir );
    when( rootDir.findDirectory( repDir ) ).thenReturn( dataServiceDir );
    when( repository.getTransformationID( transName, dataServiceDir ) ).thenReturn( objectId );

    DataServiceMeta myService = makeTestDSM( "myService", "ServiceStep", null, null, null, ServiceCacheMethod.None, 0 );

    assertNull( myService.lookupTransObjectId( null ) );
    assertNull( myService.getTransObjectId() );

    assertNull( myService.lookupTransObjectId( repository ) );
    assertNull( myService.getTransObjectId() );

    myService.setTransRepositoryPath( repDir + transName );
    assertEquals( objectId, myService.lookupTransObjectId( repository ) );
    assertEquals( objectId.getId(), myService.getTransObjectId() );

    assertEquals( objectId.getId(), myService.lookupTransObjectId( null ).getId() );
  }

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
