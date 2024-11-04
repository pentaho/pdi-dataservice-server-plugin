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


package org.pentaho.di.trans.dataservice.ui;

import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.dataservice.ui.controller.DriverDetailsDialogController;
import org.pentaho.di.ui.xul.KettleXulLoader;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.XulRunner;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.impl.AbstractXulLoader;
import org.pentaho.ui.xul.swt.SwtXulRunner;
import org.pentaho.ui.xul.swt.tags.SwtDialog;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Enumeration;
import java.util.ResourceBundle;

public class DriverDetailsDialog {
  public static final String XUL_DIALOG_ID = "driver-details-dialog";

  private static final String XUL_DIALOG_PATH = "org/pentaho/di/trans/dataservice/ui/xul/driverdetails-dialog.xul";
  private static final String XUL_DTE_DIALOG_PATH = "org/pentaho/di/trans/dataservice/ui/xul/dte-driverdetails-dialog.xul";
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

  DriverDetailsDialog( Shell parent ) throws KettleException, XulException {
    controller = new DriverDetailsDialogController();
    Document xulDocument = initXul( parent, new KettleXulLoader(), new SwtXulRunner() );
    dialog = (SwtDialog) xulDocument.getElementById( XUL_DIALOG_ID );
  }

  Document initXul( Composite parent, AbstractXulLoader xulLoader, XulRunner xulRunner ) throws KettleException {
    try {
      xulLoader.setOuterContext( parent );
      xulLoader.registerClassLoader( getClass().getClassLoader() );
      XulDomContainer container = xulLoader.loadXul( Const.isRunningOnWebspoonMode() ? XUL_DTE_DIALOG_PATH : XUL_DIALOG_PATH, resourceBundle );
      container.addEventHandler( controller );

      xulRunner.addContainer( container );
      xulRunner.initialize();
      return container.getDocumentRoot();
    } catch ( XulException xulException ) {
      throw new KettleException( "Failed to initialize DriverDetailsDialog.", xulException );
    }
  }

  SwtDialog getDialog() {
    return dialog;
  }

  void open() {
    getDialog().show();
  }

  void close() {
    getDialog().dispose();
  }
}
