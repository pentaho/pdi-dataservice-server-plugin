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
