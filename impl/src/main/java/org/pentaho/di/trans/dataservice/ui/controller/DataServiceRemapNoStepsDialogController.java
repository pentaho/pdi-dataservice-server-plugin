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


package org.pentaho.di.trans.dataservice.ui.controller;

import org.pentaho.ui.xul.swt.tags.SwtDialog;

public class DataServiceRemapNoStepsDialogController extends AbstractController {
  private static final String XUL_DIALOG_ID = "dataservice-remap-no-steps-dialog";
  private static final String NAME = "remapNoStepsDialogController";

  public DataServiceRemapNoStepsDialogController() {
    setName( NAME );
  }

  public void close() {
    getDialog().dispose();
  }

  SwtDialog getDialog() {
    return getElementById( XUL_DIALOG_ID );
  }
}
