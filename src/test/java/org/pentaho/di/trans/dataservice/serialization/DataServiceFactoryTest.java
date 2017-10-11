/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.serialization;

import com.google.common.base.Supplier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.clients.DataServiceClient;
import org.pentaho.di.trans.dataservice.clients.Query;
import org.pentaho.di.trans.dataservice.resolvers.DataServiceResolver;
import org.pentaho.di.trans.dataservice.resolvers.MetaStoreResolver;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;
import org.pentaho.osgi.kettle.repository.locator.api.KettleRepositoryLocator;

import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */

@RunWith( org.mockito.runners.MockitoJUnitRunner.class )
public class DataServiceFactoryTest extends DataServiceMetaStoreUtilTest {
  DataServiceFactory factory;

  @Mock Supplier<Repository> supplier;
  @Mock Supplier<IMetaStore> metaStoreSupplier;
  @Mock Query.Service queryService;
  @Mock KettleRepositoryLocator repositoryLocator;
  @Mock DataServiceContext context;

  public void setUp() throws Exception {
    super.setUp();

    when( supplier.get() ).thenReturn( repository );
    when( context.getMetaStoreUtil() ).thenReturn( metaStoreUtil );
    factory = new DataServiceFactory( context ) {
      @Override public Repository getRepository() {
        return supplier.get();
      }
    };
  }

  @Test
  public void testGetDataService() throws Exception {
    assertThat( factory.getDataServiceNames(), empty() );
    metaStoreUtil.save( dataService );
    metaStoreUtil.sync( transMeta, exceptionHandler );

    assertThat( factory.getDataServiceNames(), contains( DATA_SERVICE_NAME ) );
    assertThat( factory.getDataService( DATA_SERVICE_NAME ), validDataService() );
    assertThat( factory.getDataServices( exceptionHandler ), contains( validDataService() ) );

    verify( exceptionHandler, never() ).apply( any( Exception.class ) );
  }

  @Test
  public void testCreateClient() throws Exception {

    when( repositoryLocator.getRepository() ).thenReturn( repository );
    DataServiceResolver dataServiceResolver = new MetaStoreResolver( repositoryLocator, context );

    DataServiceClient client = new DataServiceClient( queryService, dataServiceResolver, Executors.newCachedThreadPool() );
    metaStoreUtil.save( dataService );
    metaStoreUtil.sync( transMeta, exceptionHandler );

    assertThat( client.getServiceInformation(), contains( hasProperty( "name", equalTo( DATA_SERVICE_NAME ) ) ) );
  }

  @Test
  public void testLocalMetaStore() throws Exception {
    DataServiceFactory.localPentahoMetaStore = new MemoryMetaStore();

    assertThat( factory.getRepository(), sameInstance( repository ) );
    assertThat( factory.getMetaStore(), sameInstance( (IMetaStore) metaStore ) );

    when( supplier.get() ).thenReturn( null );
    assertThat( factory.getRepository(), nullValue() );
    assertThat( factory.getMetaStore(), sameInstance( DataServiceFactory.localPentahoMetaStore ) );
  }
}
