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

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.List;

import static org.pentaho.di.i18n.BaseMessages.getString;

public class DataServiceDelegate {

  private static final Class<?> PKG = DataServiceDelegate.class;
  private static final Log logger = LogFactory.getLog( DataServiceDelegate.class );
  private final DataServiceContext context;
  private final Supplier<Spoon> spoonSupplier;

  private enum DefaultSpoonSupplier implements Supplier<Spoon> {
    INSTANCE;

    @Override public Spoon get() {
      return Spoon.getInstance();
    }
  }

  public static DataServiceDelegate withDefaultSpoonInstance( DataServiceContext context ) {
    return new DataServiceDelegate( context, DefaultSpoonSupplier.INSTANCE );
  }

  public DataServiceDelegate( DataServiceContext context, Spoon spoon ) {
    this( context, Suppliers.ofInstance( spoon ) );
  }

  public DataServiceDelegate( DataServiceContext context, Supplier<Spoon> spoonSupplier ) {
    this.context = context;
    this.spoonSupplier = spoonSupplier;
  }

  public void createNewDataService( String stepName ) {
    TransMeta transMeta = getSpoon().getActiveTransformation();
    if ( transMeta.hasChanged() ) {
      showSavePrompt( getString( PKG, "DataServiceDelegate.NewTransChanged.Title" ),
          getString( PKG, "DataServiceDelegate.NewTransChanged.Message" ) );
      if ( transMeta.hasChanged() ) {
        return;
      }
    }
    try {
      DataServiceDialog.create( this, transMeta, stepName ).open();
    } catch ( KettleException e ) {
      logger.error( "Unable to create a new data service", e );
    }
  }

  public void editDataService( DataServiceMeta dataService ) {
    TransMeta transMeta = dataService.getServiceTrans();
    if ( transMeta.hasChanged() ) {
      showSavePrompt( getString( PKG, "DataServiceDelegate.EditTransChanged.Title" ),
          getString( PKG, "DataServiceDelegate.EditTransChanged.Message" ) );
      if ( transMeta.hasChanged() ) {
        return;
      }
    }

    try {
      DataServiceDialog.edit( this, dataService ).open();
    } catch ( KettleException e ) {
      logger.error( "Unable to edit a data service", e );
    }
  }

  private void showSavePrompt( String title, String message ) {
    MessageDialog dialog =
        new MessageDialog( getShell(), title,  null, message, MessageDialog.QUESTION,
            new String[] {
                getString( PKG, "DataServiceDelegate.Yes.Button" ),
                getString( PKG, "DataServiceDelegate.No.Button" ) }, 0 );
    if ( dialog.open() == 0 ) {
      try {
        getSpoon().saveToFile( getSpoon().getActiveTransformation() );
      } catch ( KettleException e ) {
        logger.error( "Failed to save transformation", e );
      }
    } else {
      showError( title, getString( PKG, "DataServiceDelegate.PleaseSave.Message" ) );
    }
  }

  public void showError( String title, String text ) {
    MessageBox mb = new MessageBox( getShell(), SWT.OK | SWT.ICON_WARNING );
    mb.setText( title );
    mb.setMessage( text );
    mb.open();
  }

  public Iterable<DataServiceMeta> getDataServices( TransMeta transMeta ) throws MetaStoreException {
    return getMetaStoreUtil().getDataServices( transMeta );
  }

  public Iterable<DataServiceMeta> getDataServices( Function<Exception, Void> exceptionHandler ) {
    Spoon spoon = getSpoon();
    return getMetaStoreUtil().getDataServices( spoon.getRepository(), spoon.getMetaStore(), exceptionHandler );
  }

  public DataServiceMeta getDataService( String serviceName ) throws MetaStoreException {
    Spoon spoon = getSpoon();
    return getMetaStoreUtil().getDataService( serviceName, spoon.getRepository(), spoon.getMetaStore() );
  }

  public DataServiceMeta getDataService( String serviceName, TransMeta serviceTrans ) throws MetaStoreException {
    return getMetaStoreUtil().getDataService( serviceName, serviceTrans );
  }

  public DataServiceMeta getDataServiceByStepName( TransMeta transMeta, String stepName ) throws MetaStoreException {
    return getMetaStoreUtil().getDataServiceByStepName( transMeta, stepName );
  }

  public List<String> getDataServiceNames() throws MetaStoreException {
    return getMetaStoreUtil().getDataServiceNames( getSpoon().getMetaStore() );
  }

  public boolean saveAllowed( String serviceName, DataServiceMeta editing ) {
    // TODO: Check if data service already exists in another transformation
    return true;
  }


  public void save( DataServiceMeta dataService ) throws KettleException {
    Spoon spoon = getSpoon();
    try {
      getMetaStoreUtil().save( spoon.getRepository(), spoon.getMetaStore(), dataService );
      spoon.refreshTree();
      spoon.refreshGraph();
    } catch ( MetaStoreException e ) {
      throw new KettleException( getString( PKG, "Messages.Error.MetaStore" ) );
    }
  }
  public void removeDataService( DataServiceMeta dataService, boolean prompt ) {
    boolean shouldDelete = true;
    if ( prompt ) {
      MessageBox messageBox = new MessageBox( getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION );
      messageBox.setText( getString( PKG, "DataServiceDelegate.DeleteDataService.Title" ) );
      messageBox.setMessage( getString( PKG, "DataServiceDelegate.DeleteDataService.Message", dataService.getName() ) );
      int answerIndex = messageBox.open();
      if ( answerIndex != SWT.YES ) {
        shouldDelete = false;
      }
    }
    if ( shouldDelete ) {
      try {
        if ( dataService != null ) {
          getMetaStoreUtil().removeDataService( getSpoon().getMetaStore(), dataService );
          getSpoon().refreshTree();
        }
      } catch ( MetaStoreException e ) {
        logger.error( "Unable to remove a data service", e );
      }
    }
  }

  public void removeDataService( DataServiceMeta dataServiceMeta ) {
    removeDataService( dataServiceMeta, true );
  }

  public void testDataService( DataServiceMeta dataService ) {
    try {
      new DataServiceTestDialog( new Shell( getShell() ), dataService ).open();
    } catch ( KettleException e ) {
      logger.error( "Unable to create test data service dialog", e );
    }
  }

  public Spoon getSpoon() {
    return spoonSupplier.get();
  }

  public Shell getShell() {
    return getSpoon().getShell();
  }

  public DataServiceMetaStoreUtil getMetaStoreUtil() {
    return context.getMetaStoreUtil();
  }

  public List<PushDownFactory> getPushDownFactories() {
    return context.getPushDownFactories();
  }

  public List<AutoOptimizationService> getAutoOptimizationServices() {
    return context.getAutoOptimizationServices();
  }

}
