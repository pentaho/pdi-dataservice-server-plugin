/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.serialization.DataServiceValidationException;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.dataservice.ui.DataServiceDialog;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.di.ui.util.HelpUtils;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.components.XulMenuList;
import org.pentaho.ui.xul.components.XulTextbox;
import org.pentaho.ui.xul.swt.tags.SwtDialog;

import java.lang.reflect.InvocationTargetException;

import static org.pentaho.di.i18n.BaseMessages.getString;

public class DataServiceDialogController extends AbstractController {
  public static final String XUL_DIALOG_ID = "dataservice-dialog";

  private final DataServiceDelegate delegate;
  private final DataServiceModel model;
  private DataServiceMeta dataService;
  private static final Class<?> PKG = DataServiceDialog.class;
  private static final String NAME = "dataServiceDialogController";

  {
    setName( NAME );
  }

  public DataServiceDialogController( DataServiceModel model, DataServiceDelegate delegate ) {
    this.delegate = delegate;
    this.model = model;
  }

  public void init() throws InvocationTargetException, XulException {
    BindingFactory bindingFactory = getBindingFactory();

    XulTextbox serviceName = getElementById( "service-name" );
    XulMenuList<String> steps = getElementById( "trans-steps" );
    steps.setElements( ImmutableList.copyOf( model.getTransMeta().getStepNames() ) );

    bindingFactory.setBindingType( Binding.Type.BI_DIRECTIONAL );

    bindingFactory.createBinding( model, "serviceStep", steps, "selectedItem" ).fireSourceChanged();

    bindingFactory.createBinding( model, "serviceName", serviceName, "value" ).fireSourceChanged();
  }

  public void showTestDialog() throws XulException {
    if ( dataServiceHasNoName( model ) ) {
      return;
    }

    delegate.showTestDataServiceDialog( model.getDataService(), getDialog().getShell() );
  }

  public void saveAndClose() throws XulException {
    try {
      if ( dataServiceHasNoName( model ) ) {
        return;
      }

      String existing = dataService != null ? dataService.getName() : null;

      delegate.save( delegate.checkConflict( delegate.checkDefined( model.getDataService() ), existing ) );
      // Remove edited data service if name changed
      if ( dataService != null && !model.getServiceName().equals( existing ) ) {
        delegate.removeDataService( dataService );
      }

      // Ensure the synchronization service is installed
      delegate.createSyncService().install( model.getTransMeta() );

      close();
    } catch ( DataServiceValidationException e ) {
      error( getString( PKG, "DataServiceDialog.SaveError.Title" ), e.getMessage() );
    } catch ( Exception e ) {
      error( getString( PKG, "DataServiceDialog.SaveError.Title" ), e.getMessage() );
      getLogChannel().logError( e.getMessage(), e );
    }
  }

  public void open() {
    getDialog().show();
  }

  public void close() {
    getDialog().dispose();
  }

  @SuppressWarnings( "unused" ) // Bound via XUL
  public void showHelp() {
    HelpUtils.openHelpDialog( getDialog().getShell(),
        BaseMessages.getString( PKG, "DataServiceDialog.Help.Title" ),
        BaseMessages.getString( PKG, "DataServiceDialog.Help.Url" ), "" );
  }

  @SuppressWarnings( "unused" ) // Bound via XUL
  public void showDriverDetailsDialog() {
    delegate.showDriverDetailsDialog( getDialog().getShell() );
  }

  SwtDialog getDialog() {
    return getElementById( XUL_DIALOG_ID );
  }

  public void setDataService( DataServiceMeta dataService ) {
    this.dataService = dataService;
  }

  private boolean dataServiceHasNoName( DataServiceModel serviceModel ) throws XulException {
    boolean noName = Strings.isNullOrEmpty( serviceModel.getServiceName() );
    if ( noName ) {
      error( getString( PKG, "DataServiceDialog.NameMissingError.Title" ), getString( PKG,
          "DataServiceDialog.NameMissingError.Message" ) );
    }

    return noName;
  }
}
