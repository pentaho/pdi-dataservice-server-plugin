/*!
* Copyright 2010 - 2013 Pentaho Corporation.  All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

package com.pentaho.di.trans.dataservice;

import java.util.Arrays;
import java.util.List;

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.EnterStringDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.dialog.TransDialogPlugin;
import org.pentaho.di.ui.trans.dialog.TransDialogPluginInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.IMetaStoreElement;
import org.pentaho.metastore.api.IMetaStoreElementType;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.util.PentahoDefaults;

@TransDialogPlugin(
  id = "DataServiceTransDialogTab",
  name = "Data service transformation dialog tab plugin",
  description = "This plugin makes sure there's an extra 'Data Service' tab in the transformation settings dialog",
  i18nPackageName = "com.pentaho.di.trans.dataservice"
)
public class DataServiceTransDialogTab implements TransDialogPluginInterface {

  private static Class<?> PKG = DataServiceTransDialogTab.class;
    // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private CTabItem wDataServiceTab;
  private CCombo wServiceName;
  private CCombo wServiceStep;

  /*
  private Button wServiceOutput;
  private Button wServiceAllowOptimization;
  private CCombo wServiceCacheMethod;
  */

  @Override
  public void addTab( final TransMeta transMeta, final Shell shell, final CTabFolder wTabFolder ) {

    transMeta.setRepository( Spoon.getInstance().getRepository() );
    transMeta.setMetaStore( Spoon.getInstance().getMetaStore() );

    PropsUI props = PropsUI.getInstance();
    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    wDataServiceTab = new CTabItem( wTabFolder, SWT.NONE );
    wDataServiceTab.setText( BaseMessages.getString( PKG, "TransDialog.DataServiceTab.Label" ) );

    Composite wDataServiceComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wDataServiceComp );

    FormLayout dataServiceLayout = new FormLayout();
    dataServiceLayout.marginWidth = Const.FORM_MARGIN;
    dataServiceLayout.marginHeight = Const.FORM_MARGIN;
    wDataServiceComp.setLayout( dataServiceLayout );

    // 
    // Service name
    //
    Label wlServiceName = new Label( wDataServiceComp, SWT.LEFT );
    wlServiceName.setText( BaseMessages.getString( PKG, "TransDialog.DataServiceName.Label" ) );
    wlServiceName.setToolTipText( BaseMessages.getString( PKG, "TransDialog.DataServiceName.Tooltip" ) );
    props.setLook( wlServiceName );
    FormData fdlServiceName = new FormData();
    fdlServiceName.left = new FormAttachment( 0, 0 );
    fdlServiceName.right = new FormAttachment( middle, -margin );
    fdlServiceName.top = new FormAttachment( 0, 0 );
    wlServiceName.setLayoutData( fdlServiceName );
    wServiceName = new CCombo( wDataServiceComp, SWT.LEFT | SWT.BORDER | SWT.SINGLE );
    wServiceName.setToolTipText( BaseMessages.getString( PKG, "TransDialog.DataServiceName.Tooltip" ) );
    props.setLook( wServiceName );
    FormData fdServiceName = new FormData();
    fdServiceName.left = new FormAttachment( middle, 0 );
    fdServiceName.right = new FormAttachment( 100, 0 );
    fdServiceName.top = new FormAttachment( 0, 0 );
    wServiceName.setLayoutData( fdServiceName );
    wServiceName.setEditable( false );
    wServiceName.setItems( getDataServiceElementNames( shell, transMeta.getMetaStore() ) );
    Control lastControl = wServiceName;

    // 
    // Service step
    //
    Label wlServiceStep = new Label( wDataServiceComp, SWT.LEFT );
    wlServiceStep.setText( BaseMessages.getString( PKG, "TransDialog.DataServiceStep.Label" ) );
    wlServiceStep.setToolTipText( BaseMessages.getString( PKG, "TransDialog.DataServiceStep.Tooltip" ) );
    props.setLook( wlServiceStep );
    FormData fdlServiceStep = new FormData();
    fdlServiceStep.left = new FormAttachment( 0, 0 );
    fdlServiceStep.right = new FormAttachment( middle, -margin );
    fdlServiceStep.top = new FormAttachment( lastControl, margin );
    wlServiceStep.setLayoutData( fdlServiceStep );
    wServiceStep = new CCombo( wDataServiceComp, SWT.LEFT | SWT.BORDER | SWT.SINGLE );
    wServiceStep.setToolTipText( BaseMessages.getString( PKG, "TransDialog.DataServiceStep.Tooltip" ) );
    props.setLook( wServiceStep );
    FormData fdServiceStep = new FormData();
    fdServiceStep.left = new FormAttachment( middle, 0 );
    fdServiceStep.right = new FormAttachment( 100, 0 );
    fdServiceStep.top = new FormAttachment( lastControl, margin );
    wServiceStep.setLayoutData( fdServiceStep );
    String[] stepnames = transMeta.getStepNames();
    Arrays.sort( stepnames );
    wServiceStep.setItems( stepnames );
    lastControl = wServiceStep;

    /*
    // 
    // Cache method
    //
    Label wlServiceCacheMethod = new Label(wDataServiceComp, SWT.LEFT);
    wlServiceCacheMethod.setText(BaseMessages.getString(PKG, "TransDialog.DataServiceCacheMethod.Label")); 
    wlServiceCacheMethod.setToolTipText(BaseMessages.getString(PKG, "TransDialog.DataServiceCacheMethod.Tooltip"));
    props.setLook(wlServiceCacheMethod);
    FormData fdlServiceCacheMethod = new FormData();
    fdlServiceCacheMethod.left = new FormAttachment(0, 0);
    fdlServiceCacheMethod.right = new FormAttachment(middle, -margin);
    fdlServiceCacheMethod.top = new FormAttachment(wServiceStep, margin);
    wlServiceCacheMethod.setLayoutData(fdlServiceCacheMethod);
    wServiceCacheMethod = new CCombo(wDataServiceComp, SWT.LEFT | SWT.BORDER | SWT.SINGLE);
    wServiceCacheMethod.setToolTipText(BaseMessages.getString(PKG, "TransDialog.DataServiceCacheMethod.Tooltip"));
    props.setLook(wServiceCacheMethod);
    FormData fdServiceCacheMethod = new FormData();
    fdServiceCacheMethod.left = new FormAttachment(middle, 0);
    fdServiceCacheMethod.right = new FormAttachment(100, 0);
    fdServiceCacheMethod.top = new FormAttachment(wServiceStep, margin);
    wServiceCacheMethod.setLayoutData(fdServiceCacheMethod);
    String[] cacheMethodDescriptions = ServiceCacheMethod.getDescriptions();
    Arrays.sort(cacheMethodDescriptions);
    wServiceCacheMethod.setItems(cacheMethodDescriptions);
    
    // 
    // output service?
    //
    Label wlServiceOutput = new Label(wDataServiceComp, SWT.LEFT);
    wlServiceOutput.setText(BaseMessages.getString(PKG,Modification by user of [Getting Started Transformation]
    "TransDialog.DataServiceOutput.Label"));
    props.setLook(wlServiceOutput);
    FormData fdlServiceOutput = new FormData();
    fdlServiceOutput.left = new FormAttachment(0, 0);
    fdlServiceOutput.right = new FormAttachment(middle, -margin);
    fdlServiceOutput.top = new FormAttachment(wServiceCacheMethod, margin);
    wlServiceOutput.setLayoutData(fdlServiceOutput);
    wlServiceOutput.setEnabled(false);
    wServiceOutput = new Button(wDataServiceComp, SWT.CHECK);
    props.setLook(wServiceOutput);
    FormData fdServiceOutput = new FormData();
    fdServiceOutput.left = new FormAttachment(middle, 0);
    fdServiceOutput.right = new FormAttachment(100, 0);
    fdServiceOutput.top = new FormAttachment(wServiceCacheMethod, margin);
    wServiceOutput.setLayoutData(fdServiceOutput);
    wServiceOutput.setEnabled(false);

    // 
    // Allow optimisation?
    //
    Label wlServiceAllowOptimization = new Label(wDataServiceComp, SWT.LEFT);
    wlServiceAllowOptimization.setText(BaseMessages.getString(PKG, "TransDialog.DataServiceAllowOptimization.Label")); 
    props.setLook(wlServiceAllowOptimization);
    FormData fdlServiceAllowOptimization = new FormData();
    fdlServiceAllowOptimization.left = new FormAttachment(0, 0);
    fdlServiceAllowOptimization.right = new FormAttachment(middle, -margin);
    fdlServiceAllowOptimization.top = new FormAttachment(wServiceOutput, margin);
    wlServiceAllowOptimization.setLayoutData(fdlServiceAllowOptimization);
    wlServiceAllowOptimization.setEnabled(false);
    wServiceAllowOptimization = new Button(wDataServiceComp, SWT.CHECK);
    props.setLook(wServiceAllowOptimization);
    FormData fdServiceAllowOptimization = new FormData();
    fdServiceAllowOptimization.left = new FormAttachment(middle, 0);
    fdServiceAllowOptimization.right = new FormAttachment(100, 0);
    fdServiceAllowOptimization.top = new FormAttachment(wServiceOutput, margin);
    wServiceAllowOptimization.setLayoutData(fdServiceAllowOptimization);
    wServiceAllowOptimization.setEnabled(false);
    */

    Button wNew = new Button( wDataServiceComp, SWT.PUSH );
    props.setLook( wNew );
    wNew.setText( BaseMessages.getString( PKG, "TransDialog.NewServiceButton.Label" ) );
    FormData fdNew = new FormData();
    fdNew.left = new FormAttachment( middle, 0 );
    fdNew.top = new FormAttachment( lastControl, margin * 2 );
    wNew.setLayoutData( fdNew );
    wNew.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        String newName = createNewService( shell, transMeta.getMetaStore() );
        if ( newName != null ) {
          wServiceName.setItems( getDataServiceElementNames( shell, transMeta.getMetaStore() ) );
          wServiceName.setText( newName );
        }

      }
    } );

    Button wRemove = new Button( wDataServiceComp, SWT.PUSH );
    props.setLook( wRemove );
    wRemove.setText( BaseMessages.getString( PKG, "TransDialog.RemoveServiceButton.Label" ) );
    FormData fdRemove = new FormData();
    fdRemove.left = new FormAttachment( wNew, margin * 2 );
    fdRemove.top = new FormAttachment( lastControl, margin * 2 );
    wRemove.setLayoutData( fdRemove );
    wRemove.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        boolean removed = removeService( shell, transMeta.getMetaStore(), wServiceName.getText() );
        if ( removed ) {
          wServiceName.setItems( getDataServiceElementNames( shell, transMeta.getMetaStore() ) );
          wServiceName.setText( "" );
        }

      }
    } );

    FormData fdDataServiceComp = new FormData();
    fdDataServiceComp.left = new FormAttachment( 0, 0 );
    fdDataServiceComp.top = new FormAttachment( 0, 0 );
    fdDataServiceComp.right = new FormAttachment( 100, 0 );
    fdDataServiceComp.bottom = new FormAttachment( 100, 0 );
    wDataServiceComp.setLayoutData( fdDataServiceComp );

    wDataServiceComp.layout();
    wDataServiceTab.setControl( wDataServiceComp );
  }


  protected boolean removeService( Shell shell, IMetaStore metaStore, String elementName ) {
    MessageDialog dialog =
      new MessageDialog( shell, "Confirm removal", shell.getDisplay().getSystemImage( SWT.ICON_QUESTION ),
        "Are you sure you want to remove data service '" + elementName + "'?", SWT.NONE, new String[] { "Yes", "No" },
        1 );
    int answerIndex = dialog.open();
    if ( answerIndex == 0 ) {
      try {
        IMetaStoreElementType elementType = DataServiceMetaStoreUtil.createDataServiceElementTypeIfNeeded( metaStore );
        IMetaStoreElement element = metaStore.getElementByName( PentahoDefaults.NAMESPACE, elementType, elementName );
        if ( element != null ) {
          metaStore.deleteElement( PentahoDefaults.NAMESPACE, elementType, element.getId() );
        }
        return true;
      } catch ( MetaStoreException e ) {
        new ErrorDialog( shell, "Error", "Error deleting data service with name '" + elementName + "'", e );
        return false;
      }

    }

    return false;
  }


  protected String createNewService( Shell shell, IMetaStore metaStore ) {
    EnterStringDialog dialog = new EnterStringDialog( shell, "table1", "Enter service name",
      "Enter the name of the new data service (virtual table name)" );
    String name = dialog.open();
    if ( name != null ) {

      try {
        IMetaStoreElementType elementType = DataServiceMetaStoreUtil.createDataServiceElementTypeIfNeeded( metaStore );
        IMetaStoreElement element = metaStore.getElementByName( PentahoDefaults.NAMESPACE, elementType, name );
        if ( element != null ) {
          throw new MetaStoreException( "The data service with name '" + name + "' already exists" );
        }
      } catch ( MetaStoreException e ) {
        new ErrorDialog( shell, "Error", "Error creating new data service", e );
        return null;
      }

      return name;
    } else {
      return null;
    }
  }

  private String[] getDataServiceElementNames( Shell shell, IMetaStore metaStore ) {
    try {
      List<DataServiceMeta> dataServices = DataServiceMetaStoreUtil.getDataServices( metaStore );
      String[] names = new String[ dataServices.size() ];
      int i = 0;
      for ( DataServiceMeta dataService : dataServices ) {
        names[ i ] = dataService.getName();
        i++;
      }
      return names;
    } catch ( Exception e ) {
      e.printStackTrace();
      new ErrorDialog( shell, "Error", "Error getting list of data services", e );
      return new String[] { };
    }
  }

  @Override
  public void getData( TransMeta transMeta ) throws KettleException {
    try {
      // Data service metadata
      //
      DataServiceMeta dataService = DataServiceMetaStoreUtil.fromTransMeta( transMeta, transMeta.getMetaStore() );
      if ( dataService != null ) {
        wServiceName.setText( Const.NVL( dataService.getName(), "" ) );
        wServiceStep.setText( Const.NVL( dataService.getStepname(), "" ) );
        // wServiceOutput.setSelection(dataService.isOutput());
        // wServiceAllowOptimization.setSelection(dataService.isOptimizationAllowed());
        // wServiceCacheMethod.setText(dataService.getCacheMethod()==null ? "" : dataService.getCacheMethod()
        // .getDescription());
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unable to load data service", e );
    }
  }

  @Override
  public void ok( TransMeta transMeta ) throws KettleException {

    try {
      // Get data service details...
      //
      DataServiceMeta dataService = new DataServiceMeta();
      String serviceName = wServiceName.getText();
      String stepService = wServiceStep.getText();
      if ( serviceName.isEmpty() || stepService.isEmpty() ) {
        throw new KettleException( "Required fields are not filled!" );
      }

      dataService.setName( serviceName );
      dataService.setStepname( stepService );
      // dataService.setOutput(wServiceOutput.getSelection());
      // dataService.setOptimizationAllowed(wServiceAllowOptimization.getSelection());
      // dataService.setCacheMethod(ServiceCacheMethod.getMethodByDescription(wServiceCacheMethod.getText()));

      DataServiceMetaStoreUtil.toTransMeta( transMeta, transMeta.getMetaStore(), dataService, true );

      transMeta.setChanged();

    } catch ( Exception e ) {
      throw new KettleException( "Error reading data service metadata", e );
    }

  }

  @Override
  public CTabItem getTab() {
    return wDataServiceTab;
  }
}
