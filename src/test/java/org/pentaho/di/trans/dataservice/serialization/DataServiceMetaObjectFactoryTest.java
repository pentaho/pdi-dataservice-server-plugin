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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.optimization.cache.ServiceCache;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@RunWith( MockitoJUnitRunner.class )
public class DataServiceMetaObjectFactoryTest {

  @Mock PushDownFactory pushDownFactory;
  List<PushDownFactory> pushDownFactories = new ArrayList<>();
  DataServiceMetaObjectFactory objectFactory;

  @Test
  public void testInstantiateNoFactories() throws MetaStoreException {
    objectFactory = new DataServiceMetaObjectFactory( Collections.emptyList() );
    assertThat(
      objectFactory.instantiateClass( "java.lang.String", Collections.emptyMap() ),
      instanceOf( String.class ) );
  }

  @Test
  public void testMatchingPushdownFactory() throws MetaStoreException {
    initObjectFactory();
    objectFactory.instantiateClass( ServiceCache.class.getName(),
      Collections.emptyMap() );
    verify( pushDownFactory, times( 1 ) ).createPushDown();
  }

  @Test
  public void testNonMatchingPushdownFactory() throws MetaStoreException {
    initObjectFactory();
    try {
      objectFactory.instantiateClass( "some.nonexistent.class",
        Collections.emptyMap() );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( MetaStoreException.class ) );
    }
  }

  @Test
  public void testGetContextIsEmpty() throws MetaStoreException {
    initObjectFactory();
    assertThat( objectFactory.getContext( null ).size(), is( 0 ) );
  }

  private void initObjectFactory() {
    Class pushDownType = ServiceCache.class;
    when( pushDownFactory.getType() ).thenReturn( pushDownType );
    pushDownFactories.add( pushDownFactory );
    objectFactory = new DataServiceMetaObjectFactory( pushDownFactories );
  }

}
