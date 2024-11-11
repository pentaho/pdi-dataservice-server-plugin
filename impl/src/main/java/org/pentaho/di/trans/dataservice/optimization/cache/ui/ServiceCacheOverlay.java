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
