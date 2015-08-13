package org.pentaho.di.trans.dataservice.optimization.cache.ui;

import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.cache.ServiceCache;
import org.pentaho.di.trans.dataservice.ui.DataServiceDialog;
import org.pentaho.di.trans.dataservice.ui.controller.AbstractController;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.components.XulCheckbox;
import org.pentaho.ui.xul.components.XulTextbox;

/**
 * @author nhudak
 */
public class ServiceCacheController extends AbstractController {
  private static final String NAME = "serviceCacheCtrl";
  private final DataServiceDialog dialog;

  public ServiceCacheController( DataServiceDialog dialog ) {
    this.dialog = dialog;
    setName( NAME );
  }

  public void initBindings( PushDownOptimizationMeta meta ) {
    ServiceCache serviceCache = (ServiceCache) meta.getType();
    BindingFactory bindingFactory = createBindingFactory();

    XulCheckbox checkbox = getElementById( "service-cache-checkbox" );
    XulTextbox ttl = getElementById( "service-cache-ttl" );

    bindingFactory.setBindingType( Binding.Type.ONE_WAY );

    checkbox.setChecked( meta.isEnabled() );
    bindingFactory.createBinding( checkbox, "checked", meta, "enabled" );

    ttl.setValue( serviceCache.getTimeToLive() );
    bindingFactory.createBinding( ttl, "value", serviceCache, "timeToLive" );
  }
}
