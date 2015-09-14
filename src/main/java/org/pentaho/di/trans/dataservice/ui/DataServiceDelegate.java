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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import static org.pentaho.di.i18n.BaseMessages.getString;

public class DataServiceDelegate extends DataServiceMetaStoreUtil {
  private static final Class<?> PKG = DataServiceDelegate.class;
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
      new DataServiceDialog.Builder( transMeta ).serviceStep( stepName ).build( this ).open();
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
      new DataServiceDialog.Builder( dataService.getServiceTrans() ).edit( dataService ).build( this ).open();
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
    MessageDialog dialog =
        new MessageDialog( getShell(), title,  null, message, MessageDialog.QUESTION,
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

  public void showError( String title, String text ) {
    MessageBox mb = new MessageBox( getShell(), SWT.OK | SWT.ICON_WARNING );
    mb.setText( title );
    mb.setMessage( text );
    mb.open();
  }

  public boolean showPrompt( String title, String text ) {
    MessageBox mb = new MessageBox( getShell(), SWT.YES | SWT.NO | SWT.ICON_WARNING );
    mb.setText( title );
    mb.setMessage( text );
    return mb.open() == SWT.YES;
  }

  public DataServiceMeta getDataService( String serviceName ) throws MetaStoreException {
    Spoon spoon = getSpoon();
    return getDataService( serviceName, spoon.getRepository(), spoon.getMetaStore() );
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
      MessageBox messageBox = new MessageBox( getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION );
      messageBox.setText( getString( PKG, "DataServiceDelegate.DeleteDataService.Title" ) );
      messageBox.setMessage( getString( PKG, "DataServiceDelegate.DeleteDataService.Message", dataService.getName() ) );
      int answerIndex = messageBox.open();
      if ( answerIndex != SWT.YES ) {
        return;
      }
    }
    removeDataService( dataService );
  }

  @Override public void removeDataService( DataServiceMeta dataService ) {
    super.removeDataService( dataService );
    getSpoon().refreshTree();
    getSpoon().refreshGraph();
  }

  public void testDataService( DataServiceMeta dataService ) {
    try {
      new DataServiceTestDialog( new Shell( getShell() ), dataService ).open();
    } catch ( KettleException e ) {
      getLogChannel().logError( "Unable to create test data service dialog", e );
    }
  }

  public Spoon getSpoon() {
    return spoonSupplier.get();
  }

  public Shell getShell() {
    return getSpoon().getShell();
  }
}
