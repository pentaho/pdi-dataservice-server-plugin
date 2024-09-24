/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.di.trans.dataservice.optimization.cache.ui;

import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.cache.ServiceCache;
import org.pentaho.di.trans.dataservice.optimization.cache.ServiceCacheFactory;
import org.pentaho.di.trans.dataservice.ui.controller.AbstractController;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.components.XulCheckbox;
import org.pentaho.ui.xul.components.XulRadio;
import org.pentaho.ui.xul.components.XulTab;
import org.pentaho.ui.xul.components.XulTextbox;

import java.util.List;

import static org.pentaho.di.trans.dataservice.ui.BindingConverters.not;

/**
 * @author nhudak
 */
public class ServiceCacheController extends AbstractController {
  private static final String NAME = "serviceCacheCtrl";
  private final ServiceCacheFactory factory;

  public ServiceCacheController( ServiceCacheFactory factory ) {
    this.factory = factory;
    setName( NAME );
  }

  public void initBindings( DataServiceModel model ) {
    PushDownOptimizationMeta meta = locateServiceCacheMeta( model );
    ServiceCache serviceCache = (ServiceCache) meta.getType();
    BindingFactory bindingFactory = getBindingFactory();

    XulRadio normalModeRadio = getElementById( "regular-type-radio" );
    XulRadio streamingRadioButton = getElementById( "streaming-type-radio" );
    XulTab serviceCacheTab = getElementById( "service-cache-tab" );

    XulCheckbox serviceCacheCheckBox = getElementById( "service-cache-checkbox" );
    XulTextbox serviceCacheTextBox = getElementById( "service-cache-ttl" );

    bindingFactory.setBindingType( Binding.Type.ONE_WAY );

    serviceCacheCheckBox.setChecked( meta.isEnabled() );

    serviceCacheTab.setVisible( !model.isStreaming() );

    try {
      serviceCacheTextBox.setValue( serviceCache.getConfiguredTimeToLive() );
    } catch ( Exception e ) {
      getLogChannel().logError( "Unable to set default TTL", e );
    }
    bindingFactory.createBinding( serviceCacheTextBox, "value", serviceCache, "timeToLive" );

    bindingFactory.createBinding( serviceCacheCheckBox, "checked", meta, "enabled" );

    bindingFactory.createBinding( serviceCacheCheckBox, "checked", serviceCacheTextBox, "disabled", not() );

    bindingFactory.createBinding( normalModeRadio, "selected", serviceCacheTab, "visible" );
    bindingFactory.createBinding( streamingRadioButton, "!selected", serviceCacheTab, "visible" );
  }

  /**
   * Locate or create a pushdown optimization for service cache. Only one should exist, others will be removed if found.
   *
   * @param model Data Service model to update
   * @return The ONLY Optimization Meta with a Service Cache type
   */
  protected PushDownOptimizationMeta locateServiceCacheMeta( DataServiceModel model ) {
    List<PushDownOptimizationMeta> cacheOptimizations = model.getPushDownOptimizations( factory.getType() );

    PushDownOptimizationMeta meta;
    if ( cacheOptimizations.isEmpty() ) {
      meta = new PushDownOptimizationMeta();
      meta.setStepName( model.getServiceStep() );
      meta.setType( factory.createPushDown() );

      model.add( meta );
    } else {
      meta = cacheOptimizations.get( 0 );
    }

    if ( cacheOptimizations.size() > 1 ) {
      model.removeAll( cacheOptimizations.subList( 1, cacheOptimizations.size() ) );
    }

    return meta;
  }
}
