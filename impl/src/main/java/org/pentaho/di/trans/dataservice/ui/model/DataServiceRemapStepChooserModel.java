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



package org.pentaho.di.trans.dataservice.ui.model;

import org.pentaho.ui.xul.XulEventSourceAdapter;

public class DataServiceRemapStepChooserModel extends XulEventSourceAdapter {
  private String serviceStep;

  public String getServiceStep() {
    return serviceStep;
  }

  public void setServiceStep( String serviceStep ) {
    this.serviceStep = serviceStep;
  }
}
