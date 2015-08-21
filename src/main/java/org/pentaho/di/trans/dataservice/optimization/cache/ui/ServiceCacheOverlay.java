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
