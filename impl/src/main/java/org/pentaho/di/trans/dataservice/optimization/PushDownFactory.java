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


package org.pentaho.di.trans.dataservice.optimization;

import org.pentaho.di.trans.dataservice.ui.DataServiceDialog;

/**
 * @author nhudak
 */
public interface PushDownFactory {
  String getName();
  Class<? extends PushDownType> getType();

  PushDownType createPushDown();

  DataServiceDialog.OptimizationOverlay createOverlay();
}
