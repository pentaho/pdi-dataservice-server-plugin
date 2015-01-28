/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
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

package com.pentaho.di.trans.dataservice.ui.controller;

import com.pentaho.di.trans.dataservice.DataServiceMeta;
import com.pentaho.di.trans.dataservice.publish.BaServerConnection;
import com.pentaho.di.trans.dataservice.publish.ModelServerPublish;
import com.pentaho.di.trans.dataservice.publish.PublishHelper;
import com.pentaho.di.trans.dataservice.ui.DataServicePublishCallback;
import com.pentaho.di.trans.dataservice.ui.DataServicePublishDialog;
import com.pentaho.di.trans.dataservice.ui.model.DataServicePublishModel;
import org.pentaho.agilebi.modeler.ModelerWorkspace;
import org.pentaho.agilebi.modeler.util.ModelerWorkspaceHelper;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.gui.SpoonFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metadata.model.concept.types.LocalizedString;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.binding.DefaultBindingFactory;
import org.pentaho.ui.xul.components.WaitBoxRunnable;
import org.pentaho.ui.xul.components.XulLabel;
import org.pentaho.ui.xul.components.XulTextbox;
import org.pentaho.ui.xul.components.XulWaitBox;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class DataServicePublishController extends AbstractXulEventHandler {

  private static Class<?> PKG = DataServicePublishDialog.class;

  private final DataServicePublishModel model;

  private static final String VALID_DB_TYPE = "KettleThin";
  private static final String NAME = "dataServicePublishController";

  private ModelerWorkspace modelerWorkspace;
  private DatabaseMeta databaseMeta;

  private DataServicePublishCallback callback;

  private final DataServiceMeta dataService;
  private final TransMeta transMeta;

  public DataServicePublishController( DataServicePublishModel model,
                                       DataServiceMeta dataService,
                                       TransMeta transMeta ) {
    this.model = model;
    this.dataService = dataService;
    this.transMeta = transMeta;
    setName( NAME );
  }

  public void init() throws InvocationTargetException, XulException {
    BindingFactory bindingFactory = new DefaultBindingFactory();
    bindingFactory.setDocument( this.getXulDomContainer().getDocumentRoot() );

    createBindings( bindingFactory );
  }

  private void createBindings( BindingFactory bindingFactory ) {
    XulLabel dbServerName = (XulLabel) document.getElementById( "db-server-name-label" );
    XulTextbox baServerUrl = (XulTextbox) document.getElementById( "ba-server-url-textbox" );
    XulTextbox baServerUsername = (XulTextbox) document.getElementById( "ba-server-username-textbox" );
    XulTextbox baServerPassword = (XulTextbox) document.getElementById( "ba-server-password-textbox" );

    bindingFactory.setBindingType( Binding.Type.BI_DIRECTIONAL );
    bindingFactory.createBinding( model, "dbServerName", dbServerName, "value" );
    bindingFactory.createBinding( model, "baServerUrl", baServerUrl, "value" );
    bindingFactory.createBinding( model, "baServerUsername", baServerUsername, "value" );
    bindingFactory.createBinding( model, "baServerPassword", baServerPassword, "value" );
  }

  public void testConnection() throws XulException {

    if ( !validateInputs() ) {
      return;
    }

    XulWaitBox wait = (XulWaitBox) document.createElement( "waitbox" );
    wait.setTitle( BaseMessages.getString( PKG, "DataServicePublish.TestConnection.Label" ) );
    wait.setMessage( BaseMessages.getString( PKG, "DataServicePublish.TestConnection.Message" ) );
    wait.setIndeterminate( true );
    wait.setDialogParent( Spoon.getInstance().getShell() );
    wait.setRunnable( new WaitBoxRunnable( wait ) {

      @Override
      public void cancel() {
      }

      @Override
      public void run() {
        BaServerConnection baServerConnection = new BaServerConnection();
        baServerConnection.setUrl( model.getBaServerUrl() );
        baServerConnection.setUsername( model.getBaServerUsername() );
        baServerConnection.setPassword( model.getBaServerPassword() );

        int result = testConnection( baServerConnection );
        this.waitBox.stop();
        showResult( result );
      }
    } );
    wait.start();
  }

  protected int testConnection( BaServerConnection baServerConnection ) {
    return PublishHelper.testConnection( baServerConnection );
  }

  protected void setModelerWorkspace( ModelerWorkspace modelerWorkspace ) {
    this.modelerWorkspace = modelerWorkspace;
  }

  protected void setDatabaseMeta( DatabaseMeta databaseMeta ) {
    this.databaseMeta = databaseMeta;
  }

  public void selectDatabase() {
    try {
      setModelerWorkspace( new ModelerWorkspace( new ModelerWorkspaceHelper( LocalizedString.DEFAULT_LOCALE ) ) );

      Spoon spoon = Spoon.getInstance();

      List<DatabaseMeta> databaseMetas = transMeta.getDatabases();
      List<String> dbNames = new ArrayList<String>();
      for ( DatabaseMeta databaseMeta : databaseMetas ) {
        String pluginId = databaseMeta.getDatabaseInterface().getPluginId();
        if ( pluginId.equals( VALID_DB_TYPE ) ) {
          dbNames.add( databaseMeta.getName() );
        }
      }

      EnterSelectionDialog theDialog =
        new EnterSelectionDialog( spoon.getShell(), dbNames.toArray( new String[ dbNames.size() ] ),
          BaseMessages.getString( Spoon.class, "Spoon.ExploreDB.SelectDB.Title" ),
          BaseMessages.getString( Spoon.class, "Spoon.ExploreDB.SelectDB.Message" ),
          transMeta );
      String databaseName = theDialog.open();

      model.setDbServerName( databaseName );

      if ( databaseName != null ) {
        setDatabaseMeta( DatabaseMeta.findDatabase( databaseMetas, databaseName ) );

        XulTextbox baServerUrl = (XulTextbox) document.getElementById( "ba-server-url-textbox" );
        baServerUrl.setFocus();
      }

    } catch ( Exception e ) {
      new ErrorDialog( ( (Spoon) SpoonFactory.getInstance() ).getShell(),
        BaseMessages.getString( PKG, "DataServicePublish.Error.Label" ),
        BaseMessages.getString( PKG, "DataServicePublish.Failure.Message" ),
        e );
    }
  }

  public void publish() throws Exception {

    if ( !validateInputs() ) {
      return;
    }

    XulWaitBox wait = (XulWaitBox) document.createElement( "waitbox" );
    wait.setTitle( BaseMessages.getString( PKG, "DataServicePublish.Publishing.Label" ) );
    wait.setMessage( BaseMessages.getString( PKG, "DataServicePublish.Publishing.Message" ) );
    wait.setIndeterminate( true );
    wait.setDialogParent( Spoon.getInstance().getShell() );
    wait.setRunnable( new WaitBoxRunnable( wait ) {
      @Override
      public void cancel() {
      }

      @Override
      public void run() {
        BaServerConnection baServerConnection = new BaServerConnection();
        baServerConnection.setUrl( model.getBaServerUrl() );
        baServerConnection.setUsername( model.getBaServerUsername() );
        baServerConnection.setPassword( model.getBaServerPassword() );

        int result;

        try {
          result = publish( modelerWorkspace, baServerConnection, databaseMeta, dataService );
        } catch ( Exception e ) {
          result = ModelServerPublish.PUBLISH_FAILED;
        }

        this.waitBox.stop();
        showResult( result );
        document.invokeLater( new Runnable() {
          public void run() {
            callback.onClose();
          }
        } );
      }
    } );
    wait.start();
  }

  protected int publish( ModelerWorkspace modelerWorkspace, BaServerConnection baServerConnection,
                         DatabaseMeta databaseMeta, DataServiceMeta dataService ) throws Exception {
    return PublishHelper.publish( modelerWorkspace, baServerConnection, databaseMeta, dataService );
  }

  protected void showResult( int result ) {
    switch( result ) {
      case ModelServerPublish.PUBLISH_SUCCESS:
        SpoonFactory.getInstance().messageBox(
          BaseMessages.getString( PKG, "DataServicePublish.Success.Message" ),
          BaseMessages.getString( PKG, "DataServicePublish.Success.Label" ), false, Const.INFO );
        break;
      case ModelServerPublish.PUBLISH_FAILED:
        SpoonFactory.getInstance().messageBox(
          BaseMessages.getString( PKG, "DataServicePublish.Failure.Message" ),
          BaseMessages.getString( PKG, "DataServicePublish.Failure.Label" ), false, Const.ERROR );
        break;
      case ModelServerPublish.PUBLISH_VALID_SERVER:
        SpoonFactory.getInstance().messageBox(
          BaseMessages.getString( PKG, "DataServicePublish.ValidServer.Message" ),
          BaseMessages.getString( PKG, "DataServicePublish.Success.Label" ), false, Const.INFO );
        break;
      case ModelServerPublish.PUBLISH_INVALID_SERVER:
        SpoonFactory.getInstance().messageBox(
          BaseMessages.getString( PKG, "DataServicePublish.InvalidServer.Message" ),
          BaseMessages.getString( PKG, "DataServicePublish.Success.Label" ), false, Const.INFO );
        break;
    }
  }

  protected boolean validateInputs() {

    if ( model.getDbServerName() == null ) {
      SpoonFactory
        .getInstance()
        .messageBox(
          BaseMessages.getString( PKG, "DataServicePublish.InvalidDatabase.Message" ),
          BaseMessages.getString( PKG, "DataServicePublish.Error.Label" ), false, Const.ERROR );
      return false;
    }

    if ( model.getBaServerUrl() == null || model.getBaServerUsername() == null
      || model.getBaServerPassword() == null ) {
      SpoonFactory
        .getInstance()
        .messageBox(
          BaseMessages.getString( PKG, "DataServicePublish.InvalidServer.Message" ),
          BaseMessages.getString( PKG, "DataServicePublish.Error.Label" ), false, Const.ERROR );
      return false;
    }

    return true;
  }

  public void cancel() {
    callback.onClose();
  }

  public void setCallback( DataServicePublishCallback callback ) {
    this.callback = callback;
  }

}
