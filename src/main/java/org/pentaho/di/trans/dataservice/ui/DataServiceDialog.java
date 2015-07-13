/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2015 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package org.pentaho.di.trans.dataservice.ui;

import org.pentaho.di.trans.dataservice.DataServiceMetaStoreUtil;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.ui.controller.DataServiceDialogController;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import com.sun.istack.NotNull;
import org.eclipse.swt.widgets.Composite;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.XulRunner;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.swt.SwtXulLoader;
import org.pentaho.ui.xul.swt.SwtXulRunner;

import java.util.Enumeration;
import java.util.List;
import java.util.ResourceBundle;

public class DataServiceDialog {

  private static final String XUL_DIALOG_PATH = "org/pentaho/di/trans/dataservice/ui/xul/dataservice-dialog.xul";
  private static final String XUL_DIALOG_ID = "dataservice-dialog";
  private DataServiceDialogController dataServiceDialogController;
  private DataServiceModel dataServiceModel;
  private final Document document;
  private final XulDialog xulDialog;
  private final Composite parent;

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

  public DataServiceDialog( Composite parent, DataServiceMeta dataService, DataServiceMetaStoreUtil metaStoreUtil,
                            TransMeta transMeta, List<AutoOptimizationService> autoOptimizationServices,
                            List<PushDownFactory> pushDownFactories ) throws KettleException {
    this.parent = parent;
    dataServiceModel = new DataServiceModel();
    dataServiceDialogController =
      new DataServiceDialogController( parent, dataServiceModel, dataService, metaStoreUtil, transMeta,
        Spoon.getInstance(), autoOptimizationServices, pushDownFactories );
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
      SwtXulLoader swtLoader = new SwtXulLoader();
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
