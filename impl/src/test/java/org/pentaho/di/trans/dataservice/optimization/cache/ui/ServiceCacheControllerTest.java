/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.optimization.cache.ui;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.caching.api.PentahoCacheTemplateConfiguration;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownType;
import org.pentaho.di.trans.dataservice.optimization.cache.ServiceCache;
import org.pentaho.di.trans.dataservice.optimization.cache.ServiceCacheFactory;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.components.XulCheckbox;
import org.pentaho.ui.xul.components.XulRadio;
import org.pentaho.ui.xul.components.XulTab;
import org.pentaho.ui.xul.components.XulTextbox;
import org.pentaho.ui.xul.dom.Document;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class ServiceCacheControllerTest {

  @Mock ServiceCacheFactory factory;
  @Mock DataServiceModel model;
  @Mock XulDomContainer xulDomContainer;
  @Mock Document document;
  @Mock BindingFactory bindingFactory;
  @Mock XulCheckbox checkbox;
  @Mock XulTextbox ttl;
  @Mock XulTab serviceCacheTab;
  @Mock XulRadio regularTypeRadio;
  @Mock XulRadio streamingTypeRadio;
  @Mock PentahoCacheTemplateConfiguration cacheConfig;
  @Mock LogChannel log;

  PushDownOptimizationMeta meta;
  ServiceCache serviceCache;
  ServiceCacheController controller;

  @Before
  public void setUp() throws Exception {
    when( xulDomContainer.getDocumentRoot() ).thenReturn( document );

    when( document.getElementById( "service-cache-checkbox" ) ).thenReturn( checkbox );
    when( document.getElementById( "service-cache-ttl" ) ).thenReturn( ttl );
    when( document.getElementById( "service-cache-tab" ) ).thenReturn( serviceCacheTab );
    when( document.getElementById( "regular-type-radio" ) ).thenReturn( regularTypeRadio );
    when( document.getElementById( "streaming-type-radio" ) ).thenReturn( streamingTypeRadio );
    when( model.isStreaming() ).thenReturn( false );
  }

  @Test
  public void testBindingsNonStreaming() throws Exception {
    testBindingsAux();

    controller.initBindings( model );

    verify( checkbox ).setChecked( true );
    verify( ttl ).setValue( "1200" );
    verify( bindingFactory ).createBinding( checkbox, "checked", meta, "enabled" );
    verify( bindingFactory ).createBinding( ttl, "value", serviceCache, "timeToLive" );
    verify( serviceCacheTab ).setVisible( true );
    verify( bindingFactory ).createBinding( streamingTypeRadio, "!selected", serviceCacheTab,
      "visible" );
    verify( bindingFactory ).createBinding( regularTypeRadio, "selected", serviceCacheTab,
      "visible" );
  }

  @Test
  public void testBindingsStreaming() throws Exception {
    testBindingsAux();
    RuntimeException e = new RuntimeException();
    when( model.isStreaming() ).thenReturn( true );

    controller.initBindings( model );

    verify( checkbox ).setChecked( true );
    verify( ttl ).setValue( "1200" );
    verify( bindingFactory ).createBinding( checkbox, "checked", meta, "enabled" );
    verify( bindingFactory ).createBinding( ttl, "value", serviceCache, "timeToLive" );
    verify( serviceCacheTab ).setVisible( false );
    verify( bindingFactory ).createBinding( streamingTypeRadio, "!selected", serviceCacheTab,
      "visible" );
    verify( bindingFactory ).createBinding( regularTypeRadio, "selected", serviceCacheTab,
      "visible" );
  }

  @Test
  public void testBindingsTtlException() throws Exception {
    testBindingsAux();
    RuntimeException e = new RuntimeException();
    doThrow( e ).when( serviceCache ).getConfiguredTimeToLive();
    when( model.isStreaming() ).thenReturn( true );

    controller.initBindings( model );

    verify( log ).logError( "Unable to set default TTL", e  );
  }

  private void testBindingsAux() throws Exception {
    // Override Controller to inject our mocks
    controller = new ServiceCacheController( factory ) {
      @Override protected PushDownOptimizationMeta locateServiceCacheMeta( DataServiceModel model ) {
        assertThat( model, sameInstance( ServiceCacheControllerTest.this.model ) );
        return meta;
      }

      @Override protected LogChannelInterface getLogChannel() {
        return log;
      }
    };
    controller.setXulDomContainer( xulDomContainer );
    controller.setBindingFactory( bindingFactory );

    meta = new PushDownOptimizationMeta();
    meta.setType( serviceCache = mock( ServiceCache.class ) );

    meta.setEnabled( true );
    when( serviceCache.getConfiguredTimeToLive() ).thenReturn( "1200" );
  }

  @Test
  public void testLocateServiceCacheMeta() throws Exception {
    controller = new ServiceCacheController( factory );

    when( factory.getType() ).thenReturn( ServiceCache.class );
    when( model.getPushDownOptimizations( ServiceCache.class ) )
      .thenReturn( ImmutableList.<PushDownOptimizationMeta>of() );
    when( factory.createPushDown() ).thenReturn( serviceCache = mock( ServiceCache.class ) );

    meta = controller.locateServiceCacheMeta( model );
    assertThat( meta.isEnabled(), is( true ) );
    assertThat( meta.getType(), is( (PushDownType) serviceCache ) );

    PushDownOptimizationMeta meta2 = mock( PushDownOptimizationMeta.class );
    when( model.getPushDownOptimizations( ServiceCache.class ) ).thenReturn( ImmutableList.of( meta, meta2 ) );

    assertThat( controller.locateServiceCacheMeta( model ), is( meta ) );

    verify( model ).add( meta );
    verify( model ).removeAll( ImmutableList.of( meta2 ) );
  }
}
