/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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
