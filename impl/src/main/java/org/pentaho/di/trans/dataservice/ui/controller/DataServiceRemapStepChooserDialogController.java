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

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.dataservice.ui.DataServiceRemapStepChooserDialog;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceRemapStepChooserModel;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.components.XulLabel;
import org.pentaho.ui.xul.swt.tags.SwtDialog;
import org.pentaho.ui.xul.swt.tags.SwtListbox;

public class DataServiceRemapStepChooserDialogController extends AbstractController {
  private static final String XUL_DIALOG_ID = "dataservice-remap-step-chooser-dialog";
  private static final String NAME = "remapStepChooserController";
  private DataServiceRemapStepChooserDialog.Action action = DataServiceRemapStepChooserDialog.Action.CANCEL;
  private static Class<?> PKG = DataServiceRemapStepChooserDialog.class;
  private DataServiceMeta dataService;
  private List<String> stepNames;
  private DataServiceRemapStepChooserModel model;
  private DataServiceDelegate dataServiceDelegate;

  public DataServiceRemapStepChooserDialogController( DataServiceRemapStepChooserModel model,
      DataServiceMeta dataService,
      List<String> stepNames, DataServiceDelegate dataServiceDelegate ) {
    setName( NAME );
    this.model = model;
    this.dataService = dataService;
    this.stepNames = stepNames;
    this.dataServiceDelegate = dataServiceDelegate;
  }

  public void init() throws InvocationTargetException, XulException {
    model.setServiceStep( stepNames.get( 0 ) );
    ( (XulLabel) getElementById( "label1" ) )
        .setValue( BaseMessages.getString( PKG, "RemapStepChooserDialog.Label", dataService.getName() ) );

    SwtListbox steps = getElementById( "trans-steps" );
    steps.setElements( stepNames );

    BindingFactory bindingFactory = getBindingFactory();
    bindingFactory.setBindingType( Binding.Type.BI_DIRECTIONAL );
    bindingFactory.createBinding( model, "serviceStep", steps, "selectedItem" ).fireSourceChanged();
  }

  @SuppressWarnings( "unused" ) // Bound via xul
  public void remap() {
    dataService.setStepname( model.getServiceStep() );

    try {
      dataServiceDelegate.save( dataService );
      action = DataServiceRemapStepChooserDialog.Action.REMAP;
    } catch ( Exception e ) {
      e.printStackTrace();
      dataServiceDelegate.showError( "Error saving data service", "Error occurred while saving data service." );
      action = DataServiceRemapStepChooserDialog.Action.CANCEL;
    }
    getDialog().dispose();
  }

  public void cancel() {
    getDialog().dispose();
  }

  public DataServiceRemapStepChooserDialog.Action getAction() {
    return action;
  }

  SwtDialog getDialog() {
    return getElementById( XUL_DIALOG_ID );
  }
}
