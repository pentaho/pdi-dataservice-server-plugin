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
import org.pentaho.di.trans.dataservice.optimization.cache.ServiceCacheFactory;
import org.pentaho.di.trans.dataservice.ui.DataServiceDialog;

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

  @Override public double getPriority() {
    return 0;
  }

  @Override public void apply( DataServiceDialog dialog ) throws KettleException {
    ServiceCacheController controller = factory.createController();

    dialog.applyOverlay( this, XUL_OVERLAY ).addEventHandler( controller );

    controller.initBindings( dialog.getModel() );
  }

}
