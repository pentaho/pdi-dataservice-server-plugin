package org.pentaho.di.trans.dataservice.optimization.cache.ui;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.cache.ServiceCacheFactory;
import org.pentaho.di.trans.dataservice.ui.DataServiceDialog;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;

import java.util.List;

/**
 * @author nhudak
 */
public class ServiceCacheOverlay implements DataServiceDialog.OptimizationOverlay {
  private static final String XUL_OVERLAY =
    "/org/pentaho/di/trans/dataservice/optimization/cache/ui/service-cache-overlay.xul";

  private ServiceCacheFactory factory;

  public ServiceCacheOverlay( ServiceCacheFactory factory ) {
    this.factory = factory;
  }

  @Override public void apply( DataServiceDialog dialog ) throws KettleException {
    ServiceCacheController controller = new ServiceCacheController( dialog );

    dialog.applyOverlay( getClass().getClassLoader(), XUL_OVERLAY ).addEventHandler( controller );

    controller.initBindings( locateServiceCacheMeta( dialog.getModel() ) );
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
