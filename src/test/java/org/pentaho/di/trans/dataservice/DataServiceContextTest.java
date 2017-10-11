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

package org.pentaho.di.trans.dataservice;

import com.google.common.base.Suppliers;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.metastore.api.IMetaStore;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */

@RunWith( org.mockito.runners.MockitoJUnitRunner.class )
public class DataServiceContextTest extends BaseTest {

  @Before
  public void setUp() throws Exception {
    context = new DataServiceContext(
      pushDownFactories, autoOptimizationServices,
      cacheManager, uiFactory, logChannel
    );

    assertThat( context.getCacheManager(), sameInstance( cacheManager ) );
    assertThat( context.getUIFactory(), sameInstance( uiFactory ) );
    assertThat( context.getLogChannel(), sameInstance( logChannel ) );
    assertThat( context.getPushDownFactories(), sameInstance( pushDownFactories ) );
    assertThat( context.getAutoOptimizationServices(), sameInstance( autoOptimizationServices ) );
  }

  @Test
  public void testGetMetaStoreUtil() throws Exception {
    assertThat( context.getMetaStoreUtil(), validMetaStoreUtil() );
  }

  @Test
  public void testGetDataServiceDelegate() throws Exception {
    assertThat( context.getDataServiceDelegate(), validMetaStoreUtil() );
  }

  protected Matcher<DataServiceMetaStoreUtil> validMetaStoreUtil() {
    return allOf(
      hasProperty( "context", sameInstance( context ) ),
      hasProperty( "stepCache", sameInstance( cache ) ),
      hasProperty( "logChannel", sameInstance( logChannel ) ),
      hasProperty( "pushDownFactories", sameInstance( pushDownFactories ) )
    );
  }
}
