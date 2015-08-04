/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.ui;

import org.eclipse.swt.widgets.Composite;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.controller.DataServiceDialogController;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.xul.KettleXulLoader;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.XulRunner;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.swt.SwtXulLoader;
import org.pentaho.ui.xul.swt.SwtXulRunner;

import java.util.Enumeration;
import java.util.ResourceBundle;

public class DataServiceDialog {

  private static final String XUL_DIALOG_PATH = "org/pentaho/di/trans/dataservice/ui/xul/dataservice-dialog.xul";
  private static final String XUL_DIALOG_ID = "dataservice-dialog";
  private DataServiceDialogController dataServiceDialogController;
  private DataServiceModel dataServiceModel;
  private final Document document;
  private final XulDialog xulDialog;

  private static final Class<?> PKG = DataServiceDialog.class;

  private final ResourceBundle resourceBundle = new ResourceBundle() {
    @Override
    public Enumeration<String> getKeys() {
      return null;
    }

    @Override
    protected Object handleGetObject( String key ) {
      return BaseMessages.getString( PKG, key );
    }
  };

  public DataServiceDialog( Composite parent, DataServiceMeta dataService, TransMeta transMeta,
                            DataServiceContext context ) throws KettleException {
    dataServiceModel = new DataServiceModel();
    dataServiceDialogController =
      new DataServiceDialogController( parent, dataServiceModel, dataService, transMeta, Spoon.getInstance(), context );
    document = initXul( parent );
    xulDialog = (XulDialog) document.getElementById( XUL_DIALOG_ID );
    attachCallback();
  }

  public void open() {
    xulDialog.show();
  }

  public void close() {
    xulDialog.hide();
  }

  private void attachCallback() {
    dataServiceDialogController.setCallback( new DataServiceDialogCallback() {
      @Override public void onClose() {
        close();
      }

      @Override public void onViewStep() {
        xulDialog.show();
      }

      @Override public void onHideStep() {
        xulDialog.hide();
      }
    } );
  }

  private Document initXul( Composite parent ) throws KettleException {
    try {
      SwtXulLoader swtLoader = new KettleXulLoader();
      swtLoader.setOuterContext( parent );
      swtLoader.registerClassLoader( getClass().getClassLoader() );
      XulDomContainer container = swtLoader.loadXul( XUL_DIALOG_PATH, resourceBundle );
      container.addEventHandler( dataServiceDialogController );

      final XulRunner runner = new SwtXulRunner();
      runner.addContainer( container );
      runner.initialize();
      return container.getDocumentRoot();
    } catch ( XulException xulException ) {
      throw new KettleException( "Failed to initialize DataServicesManagerDialog.", xulException );
    }
  }
}
