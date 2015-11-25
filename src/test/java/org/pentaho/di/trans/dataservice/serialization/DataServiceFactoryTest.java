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

package org.pentaho.di.trans.dataservice.serialization;

import com.google.common.base.Supplier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.dataservice.clients.DataServiceClient;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

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

  public void setUp() throws Exception {
    super.setUp();

    when( supplier.get() ).thenReturn( repository );
    metaStoreUtil = factory = new DataServiceFactory( metaStoreUtil ) {
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
    DataServiceClient client = new DataServiceClient( factory );
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
