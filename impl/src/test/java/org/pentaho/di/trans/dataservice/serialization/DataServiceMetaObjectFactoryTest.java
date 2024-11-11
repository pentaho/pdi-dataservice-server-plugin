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


package org.pentaho.di.trans.dataservice.serialization;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
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

@RunWith( MockitoJUnitRunner.StrictStubs.class)
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
