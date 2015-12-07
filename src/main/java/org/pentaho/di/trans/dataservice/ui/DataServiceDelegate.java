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

import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.serialization.DataServiceFactory;
import org.pentaho.di.trans.dataservice.serialization.SynchronizationService;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import static org.pentaho.di.i18n.BaseMessages.getString;

public class DataServiceDelegate extends DataServiceFactory {
  private static final Class<?> PKG = DataServiceDelegate.class;
  private final Supplier<Spoon> spoonSupplier;

  protected static final Supplier<Spoon> defaultSpoonSupplier = new Supplier<Spoon>() {
    @Override public Spoon get() {
      return Spoon.getInstance();
    }
  };

  public static DataServiceDelegate withDefaultSpoonInstance( DataServiceContext context ) {
    return new DataServiceDelegate( context, defaultSpoonSupplier );
  }

  public DataServiceDelegate( DataServiceContext context, Spoon spoon ) {
    this( context, Suppliers.ofInstance( spoon ) );
  }

  public DataServiceDelegate( DataServiceContext context, Supplier<Spoon> spoonSupplier ) {
    super( context.getMetaStoreUtil() );
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
      getUiFactory().getDataServiceDialogBuilder( transMeta ).serviceStep( stepName ).build( this ).open();
    } catch ( KettleException e ) {
      getLogChannel().logError( "Unable to create a new data service", e );
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
      DataServiceDialog.Builder builder = getUiFactory().getDataServiceDialogBuilder( dataService.getServiceTrans() );
      builder.edit( dataService ).build( this ).open();
    } catch ( KettleException e ) {
      getLogChannel().logError( "Unable to edit a data service", e );
    }
  }

  public void suggestEdit( DataServiceMeta dataServiceMeta, String title, String text ) {
    TransMeta serviceTrans = dataServiceMeta.getServiceTrans();
    if ( !serviceTrans.hasChanged() && showPrompt( title, text ) ) {
      editDataService( dataServiceMeta );
      serviceTrans.setChanged();
    }
  }

  private void showSavePrompt( String title, String message ) {
    MessageDialog dialog = getUiFactory().getMessageDialog( getShell(), title,  null, message, MessageDialog.QUESTION,
            new String[] {
                getString( PKG, "DataServiceDelegate.Yes.Button" ),
                getString( PKG, "DataServiceDelegate.No.Button" ) }, 0 );
    if ( dialog.open() == 0 ) {
      try {
        getSpoon().saveToFile( getSpoon().getActiveTransformation() );
      } catch ( KettleException e ) {
        getLogChannel().logError( "Failed to save transformation", e );
      }
    } else {
      showError( title, getString( PKG, "DataServiceDelegate.PleaseSave.Message" ) );
    }
  }

  protected UIFactory getUiFactory() {
    return getContext().getUIFactory();
  }

  public SynchronizationService createSyncService() {
    return new SynchronizationService( this );
  }

  public void showError( String title, String text ) {
    MessageBox mb = getUiFactory().getMessageBox( getShell(), SWT.OK | SWT.ICON_WARNING );
    mb.setText( title );
    mb.setMessage( text );
    mb.open();
  }

  public boolean showPrompt( String title, String text ) {
    MessageBox mb = getUiFactory().getMessageBox( getShell(), SWT.YES | SWT.NO | SWT.ICON_WARNING );
    mb.setText( title );
    mb.setMessage( text );
    return mb.open() == SWT.YES;
  }

  @Override
  public void save( DataServiceMeta dataService ) throws MetaStoreException {
    Spoon spoon = getSpoon();
    super.save( dataService );
    spoon.refreshTree();
    spoon.refreshGraph();
  }

  public void removeDataService( DataServiceMeta dataService, boolean prompt ) {
    if ( prompt ) {
      MessageBox messageBox = getUiFactory().getMessageBox( getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION );
      messageBox.setText( getString( PKG, "DataServiceDelegate.DeleteDataService.Title" ) );
      messageBox.setMessage( getString( PKG, "DataServiceDelegate.DeleteDataService.Message", dataService.getName() ) );
      int answerIndex = messageBox.open();
      if ( answerIndex != SWT.YES ) {
        return;
      }
    }
    removeDataService( dataService );
  }

  @Override
  public void removeDataService( DataServiceMeta dataService ) {
    super.removeDataService( dataService );
    getSpoon().refreshTree();
    getSpoon().refreshGraph();
  }

  public void showTestDataServiceDialog( DataServiceMeta dataService ) {
    showTestDataServiceDialog( dataService, getShell() );
  }

  public void showTestDataServiceDialog( DataServiceMeta dataService, Shell shell ) {
    try {
      getUiFactory().getDataServiceTestDialog( getUiFactory().getShell( shell ), dataService ).open();
    } catch ( KettleException e ) {
      getLogChannel().logError( "Unable to create test data service dialog", e );
    }
  }

  @Override
  public Repository getRepository() {
    return getSpoon().getRepository();
  }

  // TODO use it from context menu
  public void showDriverDetailsDialog() {
    showDriverDetailsDialog( getShell() );
  }

  public void showDriverDetailsDialog( Shell shell ) {
    try {
      getUiFactory().getDriverDetailsDialog( shell ).open();
    } catch ( Exception e ) {
      getLogChannel().logError( "Unable to create driver details dialog", e );
    }
  }

  public Spoon getSpoon() {
    return spoonSupplier.get();
  }

  public Shell getShell() {
    return getSpoon().getShell();
  }

  public Display getDisplay() {
    return Objects.firstNonNull( Display.getCurrent(), Display.getDefault() );
  }

  public void syncExec( Runnable runnable ) {
    getDisplay().syncExec( runnable );
  }

}
