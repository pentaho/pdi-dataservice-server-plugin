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

package org.pentaho.di.trans.dataservice;

import org.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.ui.DataServiceDialog;
import org.pentaho.di.trans.dataservice.ui.DataServiceTestDialog;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.LastUsedFile;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPointHandler;
import org.pentaho.di.core.extension.KettleExtensionPoint;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.dialog.TransLoadProgressDialog;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.List;

public class DataServiceDelegate {

  private static final Class<?> PKG = DataServiceDelegate.class;
  private static final Log logger = LogFactory.getLog( DataServiceDelegate.class );

  private List<AutoOptimizationService> autoOptimizationServices;
  private DataServiceMetaStoreUtil metaStoreUtil;
  private List<PushDownFactory> pushDownFactories;

  public DataServiceDelegate( DataServiceMetaStoreUtil metaStoreUtil,
                              List<AutoOptimizationService> autoOptimizationServices,
                              List<PushDownFactory> pushDownFactories ) {
    this.metaStoreUtil = metaStoreUtil;
    this.autoOptimizationServices = autoOptimizationServices;
    this.pushDownFactories = pushDownFactories;
  }


  public void createNewDataService() {
    createNewDataService( null );
  }

  public void createNewDataService( String stepName ) {
    try {
      DataServiceMeta dataService = new DataServiceMeta();
      if ( stepName != null ) {
        dataService.setStepname( stepName );
      }
      TransMeta transMeta = getSpoon().getActiveTransformation();
      Repository repository = transMeta.getRepository();
      if ( repository != null ) {
        dataService.setTransRepositoryPath(
          transMeta.getRepositoryDirectory().getPath() + RepositoryDirectory.DIRECTORY_SEPARATOR + transMeta
            .getName() );
        if ( repository.getRepositoryMeta().getRepositoryCapabilities().supportsReferences() ) {
          ObjectId oid = transMeta.getObjectId();
          dataService.setTransObjectId( oid == null ? null : oid.getId() );
        } else {
          dataService
            .setTransRepositoryPath( transMeta.getRepositoryDirectory().getPath() + "/" + transMeta.getName() );
        }
      } else {
        dataService.setTransFilename( transMeta.getFilename() );
      }
      DataServiceDialog dialog =
        new DataServiceDialog( getSpoon().getShell(), dataService, metaStoreUtil, transMeta, autoOptimizationServices,
          pushDownFactories );
      dialog.open();
    } catch ( KettleException e ) {
      logger.error( "Unable to create a new data service", e );
    }
  }

  public void editDataService( DataServiceMeta dataService ) {
    try {
      TransMeta transMeta = getTransMeta( dataService );

      DataServiceDialog dataServiceManagerDialog =
        new DataServiceDialog( getSpoon().getShell(), dataService, metaStoreUtil, transMeta, autoOptimizationServices,
          pushDownFactories );
      dataServiceManagerDialog.open();
    } catch ( KettleException e ) {
      logger.error( "Unable to edit a data service", e );
    }
  }

  public void removeDataService( TransMeta transMeta, DataServiceMeta dataService, boolean prompt ) {
    boolean shouldDelete = true;
    if ( prompt ) {
      MessageBox messageBox = new MessageBox( getSpoon().getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION );
      messageBox.setText( BaseMessages.getString( PKG, "DataServicePopupMenu.DeleteMessageBox.Title" ) );
      messageBox.setMessage( BaseMessages.getString( PKG, "DataServicePopupMenu.DeleteMessageBox.Message" ) );
      int answerIndex = messageBox.open();
      if ( answerIndex != SWT.YES ) {
        shouldDelete = false;
      }
    }
    if ( shouldDelete ) {
      try {
        if ( dataService != null ) {
          IMetaStore metaStore = getSpoon().getMetaStore();
          metaStoreUtil.removeDataService( transMeta, metaStore, dataService );
          getSpoon().refreshTree();
        }
      } catch ( MetaStoreException e ) {
        logger.error( "Unable to remove a data service", e );
      }
    }
  }

  public void removeDataService( TransMeta transMeta, DataServiceMeta dataServiceMeta ) {
    removeDataService( transMeta, dataServiceMeta, true );
  }

  public void testDataService( DataServiceMeta dataService ) {
    try {
      TransMeta transMeta = dataService.lookupTransMeta( getRepository() );
      new DataServiceTestDialog( getSpoon().getShell(), dataService, transMeta ).open();
    } catch ( KettleException e ) {
      logger.error( "Unable to create test data service dialog", e );
    }
  }

  public void openTrans( TransMeta transMeta ) {
    if ( transMeta.getRepository() == null ) {
      getSpoon().openFile( transMeta.getFilename(), false );
    } else {
      TransMeta loadedTransMeta = loadTransformation( transMeta );
      if ( loadedTransMeta != null ) {
        getSpoon().props.addLastFile( LastUsedFile.FILE_TYPE_TRANSFORMATION, loadedTransMeta.getName(),
          loadedTransMeta.getRepositoryDirectory().getPath(), true, loadedTransMeta.getRepository().getName() );
        getSpoon().addMenuLast();
        loadedTransMeta.clearChanged();
        getSpoon().addTransGraph( loadedTransMeta );
      }
      getSpoon().refreshGraph();
      getSpoon().refreshTree();
    }
  }

  private TransMeta getTransMeta( DataServiceMeta dataService ) throws KettleException {
    TransMeta transMeta = dataService.lookupTransMeta( getRepository() );
    if ( getRepository() != null ) {
      transMeta = loadTransformation( transMeta );
    }

    for ( TransMeta loadedTransMeta : getSpoon().getLoadedTransformations() ) {
      if ( loadedTransMeta.equals( transMeta ) ) {
        return loadedTransMeta;
      }
    }

    ExtensionPointHandler.callExtensionPoint( getSpoon().getLog(), KettleExtensionPoint.TransAfterOpen.id, transMeta );

    return transMeta;
  }

  private TransMeta loadTransformation( TransMeta transMeta ) {
    Shell shell = getSpoon().getShell();
    Repository rep = getSpoon().getRepository();
    String name = transMeta.getName();
    RepositoryDirectoryInterface repDir = transMeta.getRepositoryDirectory();
    ObjectId objId = transMeta.getObjectId();
    TransLoadProgressDialog transLoadProgressDialog;
    // prioritize loading file by id
    if ( objId != null && !Const.isEmpty( objId.getId() ) ) {
      transLoadProgressDialog = new TransLoadProgressDialog( shell, rep, objId, null );
    } else {
      transLoadProgressDialog = new TransLoadProgressDialog( shell, rep, name, repDir, null );
    }

    return transLoadProgressDialog.open();
  }

  private Spoon getSpoon() {
    return Spoon.getInstance();
  }

  private Repository getRepository() {
    return getSpoon().getRepository();
  }
}
