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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.pentaho.di.core.Const;
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
import org.pentaho.ui.xul.components.XulRadio;
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
    XulRadio streamingModeRadio = getElementById( "streaming-type-radio" );
    XulRadio normalModeRadio = getElementById( "regular-type-radio" );

    steps.setElements( ImmutableList.copyOf( model.getTransMeta().getStepNames() ) );

    bindingFactory.setBindingType( Binding.Type.BI_DIRECTIONAL );

    bindingFactory.createBinding( model, "serviceStep", steps, "selectedItem" ).fireSourceChanged();

    bindingFactory.createBinding( model, "serviceName", serviceName, "value" ).fireSourceChanged();

    bindingFactory.createBinding( model, "streaming", streamingModeRadio, "selected" ).fireSourceChanged();
    bindingFactory.createBinding( model, "!streaming", normalModeRadio, "selected" ).fireSourceChanged();
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
    String title = BaseMessages.getString( PKG, "DataServiceDialog.Help.Title" );
    String docUrl = Const.getDocUrl( BaseMessages.getString( PKG, "DataServiceDialog.Help.Url" ) );
    String header = "";

    HelpUtils.openHelpDialog( getDialog().getShell(), title, docUrl, header );
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
