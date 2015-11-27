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

import java.util.Collections;
import java.util.Enumeration;
import java.util.ResourceBundle;

import javax.annotation.Nonnull;

import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.dataservice.ui.controller.DriverDetailsDialogController;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.xul.KettleXulLoader;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.XulRunner;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.swt.SwtXulLoader;
import org.pentaho.ui.xul.swt.SwtXulRunner;
import org.pentaho.ui.xul.swt.tags.SwtDialog;

public class DriverDetailsDialog {
  public static final String XUL_DIALOG_ID = "driver-details-dialog";

  private static final String XUL_DIALOG_PATH = "org/pentaho/di/trans/dataservice/ui/xul/driverdetails-dialog.xul";
  private final DriverDetailsDialogController controller;
  private final SwtDialog dialog;
  private static final Class<?> PKG = DriverDetailsDialog.class;
  private final ResourceBundle resourceBundle = new ResourceBundle() {
    @Override
    @Nonnull
    public Enumeration<String> getKeys() {
      return Collections.emptyEnumeration();
    }

    @Override
    protected Object handleGetObject( @Nonnull String key ) {
      return BaseMessages.getString( PKG, key );
    }
  };

  DriverDetailsDialog( Shell parent ) throws KettleException {
    try {
      controller = new DriverDetailsDialogController();
      Document xulDocument = initXul( parent );
      dialog = (SwtDialog) xulDocument.getElementById( XUL_DIALOG_ID );
    } catch ( KettleException ke ) {
      new ErrorDialog( parent, BaseMessages.getString( PKG, "DataServiceTest.TestDataServiceError.Title" ),
          BaseMessages.getString( PKG, "DataServiceTest.TestDataServiceError.Message" ), ke );
      throw ke;
    }
  }

  private Document initXul( Composite parent ) throws KettleException {
    try {
      SwtXulLoader swtLoader = new KettleXulLoader();
      swtLoader.setOuterContext( parent );
      swtLoader.registerClassLoader( getClass().getClassLoader() );
      XulDomContainer container = swtLoader.loadXul( XUL_DIALOG_PATH, resourceBundle );
      container.addEventHandler( controller );

      final XulRunner runner = new SwtXulRunner();
      runner.addContainer( container );
      runner.initialize();
      return container.getDocumentRoot();
    } catch ( XulException xulException ) {
      throw new KettleException( "Failed to initialize DataServicesTestDialog.",
          xulException );
    }
  }

  void open() {
    dialog.show();
  }

  void close() {
    dialog.dispose();
  }
}
