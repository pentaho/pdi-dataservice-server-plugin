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

package org.pentaho.di.trans.dataservice.ui.controller;

import java.util.List;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.dataservice.ui.DataServiceRemapConfirmationDialog;
import org.pentaho.ui.xul.components.XulLabel;
import org.pentaho.ui.xul.swt.tags.SwtDialog;

public class DataServiceRemapConfirmationDialogController extends AbstractController {
  private static final String XUL_DIALOG_ID = "dataservice-remap-confirmation-dialog";
  private static final String NAME = "remapConfirmationDialogController";
  private DataServiceRemapConfirmationDialog.Action action = DataServiceRemapConfirmationDialog.Action.CANCEL;
  private static Class<?> PKG = DataServiceRemapConfirmationDialog.class;
  private DataServiceMeta dataService;
  private List<String> remainingStepNames;
  private DataServiceDelegate dataServiceDelegate;

  public DataServiceRemapConfirmationDialogController( DataServiceMeta dataService, List<String> remainingStepNames,
      DataServiceDelegate dataServiceDelegate ) {
    setName( NAME );
    this.dataService = dataService;
    this.remainingStepNames = remainingStepNames;
    this.dataServiceDelegate = dataServiceDelegate;
  }

  public void init() {
    ( (XulLabel) getElementById( "label1" ) )
        .setValue( BaseMessages.getString( PKG, "RemapConfirmationDialog.Label1", dataService.getStepname(),
            dataService.getName() ) );
  }

  @SuppressWarnings( "unused" ) // Bound via xul
  public void remap() {
    if ( remainingStepNames.isEmpty() ) {
      dataServiceDelegate.showRemapNoStepsDialog( getDialog().getShell() );
      return;
    }

    action = DataServiceRemapConfirmationDialog.Action.REMAP;
    getDialog().dispose();
  }

  public void delete() {
    action = DataServiceRemapConfirmationDialog.Action.DELETE;
    getDialog().dispose();
  }

  public void cancel() {
    getDialog().dispose();
  }

  public DataServiceRemapConfirmationDialog.Action getAction() {
    return action;
  }

  SwtDialog getDialog() {
    return getElementById( XUL_DIALOG_ID );
  }
}
