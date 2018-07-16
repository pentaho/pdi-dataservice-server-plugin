/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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
import java.util.List;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.serialization.DataServiceFactory;
import org.pentaho.di.trans.dataservice.serialization.DataServiceReferenceSynchronizer;
import org.pentaho.di.trans.dataservice.serialization.SynchronizationListener;
import org.pentaho.di.trans.dataservice.ui.menu.DataServiceFolderProvider;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

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

  public DataServiceDelegate( DataServiceContext context ) {
    super( context );
    this.spoonSupplier = null;
  }

  public DataServiceDelegate( DataServiceContext context, Spoon spoon ) {
    this( context, Suppliers.ofInstance( spoon ) );
  }

  public DataServiceDelegate( DataServiceContext context, Supplier<Spoon> spoonSupplier ) {
    super( context );
    this.spoonSupplier = spoonSupplier;
  }

  /**
   * Creates a new data service, where step with {@code stepName}
   * is a service step.
   *
   * @param stepName
   *    name of the step, that will be a service step.
   *    {@code null} is acceptable value.
   *
   */
  public void createNewDataService( String stepName ) {
    if ( stepName == null ) {
      StepMeta stepMeta = getLastStepOfActiveTrans();
      if ( stepMeta != null ) {
        stepName = stepMeta.getName();
      }
    }

    TransMeta transMeta = getSpoon().getActiveTransformation();
    if ( transMeta.hasChanged() ) {
      showSavePrompt( BaseMessages.getString( PKG, "DataServiceDelegate.NewTransChanged.Title" ),
        BaseMessages.getString( PKG, "DataServiceDelegate.NewTransChanged.Message" ) );
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
      showSavePrompt( BaseMessages.getString( PKG, "DataServiceDelegate.EditTransChanged.Title" ),
        BaseMessages.getString( PKG, "DataServiceDelegate.EditTransChanged.Message" ) );
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
    if ( !serviceTrans.hasChanged() ) {
      if ( showPrompt( title, text ) ) {
        editDataService( dataServiceMeta );
        serviceTrans.setChanged();
      } else {
        //can not exist duplicate, delete dataService and save transformation (PDI-15584)
        try {
          deleteDataServiceElementAndCleanCache( dataServiceMeta, serviceTrans );
          serviceTrans.setChanged();
        } catch ( MetaStoreException e ) {
          getLogChannel().logBasic( e.getMessage() );
        }
      }
    }
  }

  private void showSavePrompt( String title, String message ) {
    MessageDialog dialog = getUiFactory().getMessageDialog( getShell(), title, null, message, MessageDialog.QUESTION,
      new String[] {
        BaseMessages.getString( PKG, "DataServiceDelegate.Yes.Button" ),
        BaseMessages.getString( PKG, "DataServiceDelegate.No.Button" ) }, 0 );
    if ( dialog.open() == 0 ) {
      try {
        getSpoon().saveToFile( getSpoon().getActiveTransformation() );
      } catch ( KettleException e ) {
        getLogChannel().logError( "Failed to save transformation", e );
      }
    } else {
      showError( title, BaseMessages.getString( PKG, "DataServiceDelegate.PleaseSave.Message" ) );
    }
  }

  protected UIFactory getUiFactory() {
    return getContext().getUIFactory();
  }

  public SynchronizationListener createSyncService() {
    return new SynchronizationListener( this, new DataServiceReferenceSynchronizer( getContext() ) );
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
    spoon.refreshTree( DataServiceFolderProvider.STRING_DATA_SERVICES );
    spoon.refreshGraph();
  }

  public void removeDataService( DataServiceMeta dataService, boolean prompt ) {
    if ( prompt ) {
      MessageBox messageBox = getUiFactory().getMessageBox( getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION );
      messageBox.setText( BaseMessages.getString( PKG, "DataServiceDelegate.DeleteDataService.Title" ) );
      messageBox.setMessage( BaseMessages.getString( PKG, "DataServiceDelegate.DeleteDataService.Message", dataService.getName() ) );
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
    getSpoon().refreshTree( DataServiceFolderProvider.STRING_DATA_SERVICES );
    getSpoon().refreshGraph();
  }

  public void showTestDataServiceDialog( DataServiceMeta dataService ) {
    showTestDataServiceDialog( dataService, getShell() );
  }

  public void showTestDataServiceDialog( DataServiceMeta dataService, Shell shell ) {
    try {
      getUiFactory().getDataServiceTestDialog( getUiFactory().getShell( shell ), dataService, context ).open();
    } catch ( KettleException e ) {
      getLogChannel().logError( "Unable to create test data service dialog", e );
    }
  }

  @Override
  public Repository getRepository() {
    return getSpoon().getRepository();
  }

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

  public DataServiceRemapConfirmationDialog.Action showRemapConfirmationDialog( DataServiceMeta dataService,
      List<String> remainingStepNames ) {
    try {
      DataServiceRemapConfirmationDialog
          dialog = getUiFactory().getRemapConfirmationDialog( getShell(), dataService, remainingStepNames, this );
      dialog.open();
      return dialog.getAction();
    } catch ( KettleException e ) {
      getLogChannel().logError( "Unable to create remap data service dialog", e );
    }

    return DataServiceRemapConfirmationDialog.Action.CANCEL;
  }

  public DataServiceRemapStepChooserDialog.Action showRemapStepChooserDialog( DataServiceMeta dataService,
      List<String> remainingStepNames, TransMeta trans ) {
    try {
      DataServiceRemapStepChooserDialog
          dialog = getUiFactory().getRemapStepChooserDialog( getShell(), dataService, remainingStepNames, this );
      dialog.open();
      return dialog.getAction();
    } catch ( KettleException e ) {
      getLogChannel().logError( "Unable to create remap data service dialog", e );
    }

    return DataServiceRemapStepChooserDialog.Action.CANCEL;
  }

  public void showRemapNoStepsDialog( Shell shell ) {
    try {
      getUiFactory().getRemapNoStepsDialog( shell ).open();
    } catch ( KettleException e ) {
      getLogChannel().logError( "Unable to create no steps for remap dialog", e );
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


  StepMeta getLastStepOfActiveTrans() {
    StepMeta step = null;

    TransMeta activeTrans = getSpoon().getActiveTransformation();

    if ( activeTrans != null ) {
      List<StepMeta> steps = activeTrans.getSteps();
      if ( steps != null && !steps.isEmpty() ) {
        step = steps.get( steps.size() - 1 );
      }
    }

    return step;
  }
}
