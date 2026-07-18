/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package org.pentaho.di.trans.dataservice.streaming.ui;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.dataservice.ui.DataServiceDialog;

/**
 *Streaming UI overlay
 */
public class StreamingOverlay implements DataServiceDialog.OptimizationOverlay {
  private static final String XUL_OVERLAY =
    "/org/pentaho/di/trans/dataservice/streaming/ui/streaming-overlay.xul";

  private StreamingController controller;

  public StreamingOverlay( StreamingController controller ) {
    this.controller = controller;
  }

  @Override public double getPriority() {
    return 0;
  }

  @Override public void apply( DataServiceDialog dialog ) throws KettleException {
    dialog.applyOverlay( this, XUL_OVERLAY ).addEventHandler( controller );

    try {
      controller.initBindings( dialog.getModel() );
    } catch ( Exception e ) {
      throw new KettleException( e );
    }

  }

}
