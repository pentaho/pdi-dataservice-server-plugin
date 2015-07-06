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

package org.pentaho.di.trans.dataservice;

import com.google.common.collect.Lists;
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
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownType;
import org.pentaho.di.trans.dataservice.optimization.cache.ServiceCache;
import org.pentaho.di.trans.dataservice.ui.DataServiceDialog;
import org.pentaho.di.trans.dataservice.ui.DataServiceTestDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.dialog.TransLoadProgressDialog;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

public class DataServiceDelegate {

  private static final Class<?> PKG = DataServiceDelegate.class;
  private static final Log logger = LogFactory.getLog( DataServiceDelegate.class );
  private final DataServiceContext context;
  private final DataServiceMetaStoreUtil metaStoreUtil;

  public DataServiceDelegate( DataServiceContext context ) {
    this.context = context;
    metaStoreUtil = context.getMetaStoreUtil();
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

      for ( PushDownFactory pushDownFactory : context.getPushDownFactories() ) {
        if ( pushDownFactory.getType().equals( ServiceCache.class ) ) {
          PushDownType pushDown = pushDownFactory.createPushDown();

          PushDownOptimizationMeta pushDownOptimizationMeta = new PushDownOptimizationMeta();
          pushDownOptimizationMeta.setName( "Default Cache Optimization" );
          pushDownOptimizationMeta.setStepName( dataService.getStepname() );
          pushDownOptimizationMeta.setType( pushDown );

          dataService.setPushDownOptimizationMeta( Lists.newArrayList( pushDownOptimizationMeta ) );
        }
      }

      DataServiceDialog dialog =
        new DataServiceDialog( getSpoon().getShell(), dataService, transMeta, context );
      dialog.open();
    } catch ( KettleException e ) {
      logger.error( "Unable to create a new data service", e );
    }
  }

  public void editDataService( DataServiceMeta dataService ) {
    try {
      TransMeta transMeta = getTransMeta( dataService );

      DataServiceDialog dataServiceManagerDialog =
        new DataServiceDialog( getSpoon().getShell(), dataService, transMeta, context );
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
