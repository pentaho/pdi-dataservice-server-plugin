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

package com.pentaho.di.trans.dataservice;

import com.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptDialog;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import com.pentaho.di.trans.dataservice.optimization.PushDownType;
import com.pentaho.di.trans.dataservice.ui.DataServiceTestDialog;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.KettleRepositoryLostException;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.EnterStringDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.ShowMessageDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.dialog.TransDialogPlugin;
import org.pentaho.di.ui.trans.dialog.TransDialogPluginInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.IMetaStoreElement;
import org.pentaho.metastore.api.IMetaStoreElementType;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.util.PentahoDefaults;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

  private Button testButton;
  private Button autoOptButton;
  private TableView optimizationListTable;
  private List<PushDownOptimizationMeta> optimizationList = new ArrayList<PushDownOptimizationMeta>();

  private static final Log logger = LogFactory.getLog( DataServiceTransDialogTab.class );
  private ToolItem editButton;
  private ToolItem deleteButton;

  private int ENABLED_COMBO_COLUMN = 4;

  private List<AutoOptimizationService> autoOptimizationServices;

  @Override
  public void addTab( final TransMeta transMeta, final Shell shell, final CTabFolder wTabFolder ) {
    transMeta.setRepository( Spoon.getInstance().getRepository() );
    transMeta.setMetaStore( Spoon.getInstance().getMetaStore() );

    final PropsUI props = PropsUI.getInstance();
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

    Group serviceGroup = new Group( wDataServiceComp, SWT.SHADOW_IN );
    FormLayout serviceGroupLayout = new FormLayout();
    serviceGroupLayout.marginHeight = Const.FORM_MARGIN * 2;
    serviceGroup.setLayout( serviceGroupLayout );
    props.setLook( serviceGroup );

    FormData fdServiceGroup = new FormData();
    fdServiceGroup.top = new FormAttachment( 0, margin * 5 );
    fdServiceGroup.left = new FormAttachment( 10, 0 );
    fdServiceGroup.right = new FormAttachment( 90, 0 );
    serviceGroup.setLayoutData( fdServiceGroup );

    serviceGroup.setText( BaseMessages.getString( PKG, "TransDialog.ServiceGroup.Label" ) );

    // 
    // Service name
    //
    Label wlServiceName = new Label( serviceGroup, SWT.RIGHT );
    wlServiceName.setText( BaseMessages.getString( PKG, "TransDialog.DataServiceName.Label" ) );
    wlServiceName.setToolTipText( BaseMessages.getString( PKG, "TransDialog.DataServiceName.Tooltip" ) );
    props.setLook( wlServiceName );
    FormData fdlServiceName = new FormData();
    fdlServiceName.right = new FormAttachment( middle, -margin );
    fdlServiceName.top = new FormAttachment( 0, 0 );
    wlServiceName.setLayoutData( fdlServiceName );
    wServiceName = new CCombo( serviceGroup, SWT.LEFT | SWT.BORDER | SWT.SINGLE );
    wServiceName.setToolTipText( BaseMessages.getString( PKG, "TransDialog.DataServiceName.Tooltip" ) );
    props.setLook( wServiceName );
    FormData fdServiceName = new FormData();
    fdServiceName.left = new FormAttachment( middle, 0 );
    fdServiceName.right = new FormAttachment( 65, 0 );
    fdServiceName.top = new FormAttachment( 0, 0 );
    wServiceName.setLayoutData( fdServiceName );
    wServiceName.setEditable( false );
    wServiceName.setItems( getDataServiceElementNames( shell, transMeta.getMetaStore() ) );
    Control lastControl = wServiceName;
    wServiceName.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        try {
          if ( wServiceName.getText() != null ) {
            DataServiceMeta selectedServiceMeta =
              DataServiceMeta.getMetaStoreFactory( transMeta.getMetaStore() )
                .loadElement( wServiceName.getText() );
            optimizationList = selectedServiceMeta.getPushDownOptimizationMeta();
            refreshOptimizationList();
          }
        } catch ( MetaStoreException e ) {
          logger.error(
            String.format( "Failed to load service named '%s'", wServiceName.getText() ), e );
        }
        updateButtons();
      }
    } );

    Button wNew = new Button( serviceGroup, SWT.PUSH );
    props.setLook( wNew );
    wNew.setText( BaseMessages.getString( PKG, "TransDialog.NewServiceButton.Label" ) );
    FormData fdNew = new FormData();
    fdNew.left = new FormAttachment( wServiceName, margin * 2 );
    fdNew.top = new FormAttachment( 0, 0 );
    wNew.setLayoutData( fdNew );
    wNew.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        String newName = createNewService( shell, transMeta.getMetaStore() );
        if ( newName != null ) {
          wServiceName.setItems( getDataServiceElementNames( shell, transMeta.getMetaStore() ) );
          wServiceName.setText( newName );
          // clear out any defined optimizations
          optimizationList = new ArrayList<PushDownOptimizationMeta>();
          refreshOptimizationList();
        }
      }
    } );

    Button wRename = new Button( serviceGroup, SWT.PUSH );
    props.setLook( wRename );
    wRename.setText( BaseMessages.getString( PKG, "TransDialog.RenameServiceButton.Label" ) );
    FormData fdRename = new FormData();
    fdRename.left = new FormAttachment( wNew, margin * 2 );
    fdRename.top = new FormAttachment( 0, 0 );
    wRename.setLayoutData( fdRename );
    wRename.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        String newName = renameService( shell, transMeta.getMetaStore(), wServiceName.getText() );
        if ( newName != null ) {
          wServiceName.setItems( getDataServiceElementNames( shell, transMeta.getMetaStore() ) );
          wServiceName.setText( newName );
        }
      }
    } );


    Button wRemove = new Button( serviceGroup, SWT.PUSH );
    props.setLook( wRemove );
    wRemove.setText( BaseMessages.getString( PKG, "TransDialog.RemoveServiceButton.Label" ) );
    FormData fdRemove = new FormData();
    fdRemove.left = new FormAttachment( wRename, margin * 2 );
    fdRemove.top = new FormAttachment( 0, 0 );
    wRemove.setLayoutData( fdRemove );
    wRemove.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        boolean removed = removeService( shell, transMeta.getMetaStore(), wServiceName.getText() );
        if ( removed ) {
          wServiceName.setItems( getDataServiceElementNames( shell, transMeta.getMetaStore() ) );
          //we should set both fields empty, because if one empty then second empty
          wServiceName.setText( "" );
          wServiceStep.setText( "" );
        }
      }
    } );
    lastControl = wServiceName;

    //
    // Service step
    //
    Label wlServiceStep = new Label( serviceGroup, SWT.RIGHT );
    wlServiceStep.setText( BaseMessages.getString( PKG, "TransDialog.DataServiceStep.Label" ) );
    wlServiceStep.setToolTipText( BaseMessages.getString( PKG, "TransDialog.DataServiceStep.Tooltip" ) );
    props.setLook( wlServiceStep );
    FormData fdlServiceStep = new FormData();
    fdlServiceStep.right = new FormAttachment( middle, -margin );
    fdlServiceStep.top = new FormAttachment( lastControl, margin );
    wlServiceStep.setLayoutData( fdlServiceStep );
    wServiceStep = new CCombo( serviceGroup, SWT.LEFT | SWT.BORDER | SWT.SINGLE );
    wServiceStep.setToolTipText( BaseMessages.getString( PKG, "TransDialog.DataServiceStep.Tooltip" ) );
    props.setLook( wServiceStep );
    FormData fdServiceStep = new FormData();
    fdServiceStep.left = new FormAttachment( middle, 0 );
    fdServiceStep.right = new FormAttachment( 65, 0 );
    fdServiceStep.top = new FormAttachment( lastControl, margin );
    wServiceStep.setLayoutData( fdServiceStep );
    String[] stepnames = transMeta.getStepNames();
    Arrays.sort( stepnames );
    wServiceStep.setItems( stepnames );
    wServiceStep.addSelectionListener(
      new SelectionAdapter() {
        @Override
        public void widgetSelected( SelectionEvent e ) {
          updateButtons();
        }
      }
    );

    lastControl = wServiceStep;

    autoOptButton = new Button( serviceGroup, SWT.PUSH );
    props.setLook( autoOptButton );
    autoOptButton.setText( BaseMessages.getString( PKG, "TransDialog.FindOptimizations.Label" ) );
    FormData fdPublish = new FormData();
    fdPublish.left = new FormAttachment( middle, 0 );
    fdPublish.right = new FormAttachment( 65, 0 );
    fdPublish.top = new FormAttachment( lastControl, margin );
    autoOptButton.setLayoutData( fdPublish );

    autoOptButton.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        try {
          DataServiceMeta dataServiceMeta = getDataServiceMeta();
          for ( AutoOptimizationService autoOptimizationService : getAutoOptimizationServices() ) {
            optimizationList.addAll( autoOptimizationService.apply( transMeta, dataServiceMeta ) );
          }
        } catch ( KettleException e ) {
          logger.error( BaseMessages.getString( PKG, "TransDialog.FindOptimizations.Error" ), e );
        }
        refreshOptimizationList();
      }
    } );

    Group optimizationGroup = new Group( wDataServiceComp, SWT.SHADOW_IN );
    optimizationGroup.setLayout( new FormLayout() );
    props.setLook( optimizationGroup );

    FormData fdOptGroup = new FormData();
    fdOptGroup.top = new FormAttachment( serviceGroup, margin * 5 );
    fdOptGroup.left = new FormAttachment( 10, 0 );
    fdOptGroup.right = new FormAttachment( 90, 0 );
    fdOptGroup.bottom = new FormAttachment( 90, 0 );
    optimizationGroup.setLayoutData( fdOptGroup );

    optimizationGroup.setText( BaseMessages.getString( PKG, "TransDialog.PushDownOptGroup.Label" )  );

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "TransDialog.OptNameColumn.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "TransDialog.StepNameColumn.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "TransDialog.OptMethodColumn.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "TransDialog.StateColumn.Label" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO,
          new String[] {
            BaseMessages.getString( PKG, "TransDialog.Enabled.Value" ),
            BaseMessages.getString( PKG, "TransDialog.Disabled.Value" ) },
          true ) };

    optimizationListTable = new TableView(
      transMeta, optimizationGroup, SWT.SINGLE | SWT.BORDER | SWT.FULL_SELECTION, colinf, 0, null, props );

    ToolBar toolBar = new ToolBar( optimizationGroup, SWT.FLAT );
    FormData fdEditBtn = new FormData();
    fdEditBtn.right = new FormAttachment( 99 );
    toolBar.setLayoutData( fdEditBtn );
    props.setLook( toolBar );

    editButton = new ToolItem( toolBar, SWT.PUSH );
    editButton.setEnabled( false );
    editButton.setImage( GUIResource.getInstance().getImageEdit() );
    editButton.setWidth( GUIResource.getInstance().getImageEdit().getBounds().width );
    final ToolItem addButton = new ToolItem( toolBar, SWT.FLAT );
    addButton.setImage( GUIResource.getInstance().getImageAdd() );
    addButton.setWidth( GUIResource.getInstance().getImageAdd().getBounds().width );
    deleteButton = new ToolItem( toolBar, SWT.FLAT );
    deleteButton.setEnabled( false );
    deleteButton.setImage( GUIResource.getInstance().getImageDelete() );
    deleteButton.setWidth( GUIResource.getInstance().getImageDelete().getBounds().width );


    editButton.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        if ( optimizationListTable.getSelectionIndex() < 0
          || optimizationListTable.getSelectionIndex() >= optimizationList.size() ) {
          return;
        }
        PushDownOptDialog dialog = new PushDownOptDialog( shell, props, transMeta,
          optimizationList.get( optimizationListTable.getSelectionIndex() ) );
        if ( dialog.open() == SWT.OK ) {
          refreshOptimizationList();
        }
      }
    } );

    deleteButton.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        int selectedIndex = optimizationListTable.getSelectionIndex();
        if ( selectedIndex >= 0 && optimizationList.size() > selectedIndex ) {
          optimizationList.remove( selectedIndex );
          refreshOptimizationList();
        }
      }
    } );

    addButton.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        PushDownOptimizationMeta optMeta = new PushDownOptimizationMeta();
        PushDownOptDialog dialog = new PushDownOptDialog( shell, props, transMeta, optMeta );
        if ( dialog.open() == SWT.OK ) {
          optimizationList.add( optMeta );
          refreshOptimizationList();
        }
      }
    } );

    optimizationListTable.table.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        super.widgetSelected( e );
        TableItem firstItem = optimizationListTable.table.getItem( 0 );
        if ( optimizationListTable.table.getSelectionIndex() > 0
          || ( firstItem != null && firstItem.getText( 1 ).trim().length() > 0 ) ) {
          editButton.setEnabled( true );
          deleteButton.setEnabled( true );
        }
      }
    } );
    FormData fdOptTable = new FormData();
    fdOptTable.top = new FormAttachment( toolBar, margin * 2 );
    fdOptTable.left = new FormAttachment( 0, margin );
    fdOptTable.right = new FormAttachment( 100, -margin );
    fdOptTable.bottom = new FormAttachment( 90, -margin );
    optimizationListTable.setLayoutData( fdOptTable );


    testButton = new Button( optimizationGroup, SWT.PUSH );
    props.setLook( testButton );
    testButton.setText( BaseMessages.getString( PKG, "TransDialog.TestButton.Label" ) );
    FormData fdTest = new FormData();

    fdTest.left = new FormAttachment( optimizationListTable, 0, SWT.CENTER );
    fdTest.top = new FormAttachment( optimizationListTable, margin * 2 );
    testButton.setLayoutData( fdTest );
    testButton.setEnabled( false );
    testButton.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent event ) {
        try {
          new DataServiceTestDialog( shell, getDataServiceMeta(), transMeta ).open();
        } catch ( KettleException e ) {
          new ErrorDialog( shell, BaseMessages.getString( PKG, "TransDialog.ErrorDialog.Label" ),
            BaseMessages.getString( PKG, "TransDialog.TestDialogInitError.Label" ), e );
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

  private void updateButtons() {
    String serviceName = wServiceName.getText();
    String stepName = wServiceStep.getText();
    boolean enabled = serviceName != null && stepName != null && serviceName.length() > 0 && stepName.length() > 0;
    testButton.setEnabled( enabled );
    autoOptButton.setEnabled( enabled );
  }

  protected String renameService( Shell shell, IMetaStore metaStore, String elementName ) {
    EnterStringDialog dialog =
      new EnterStringDialog( shell, elementName, BaseMessages.getString( PKG, "TransDialog.RenameServiceDialog.Label" ),
        BaseMessages.getString( PKG, "TransDialog.RenameServiceDialog.Description" ) );
    String newName = dialog.open();
    if ( newName != null ) {
      try {
        IMetaStoreElementType elementType = DataServiceMetaStoreUtil.createDataServiceElementTypeIfNeeded( metaStore );
        IMetaStoreElement element = metaStore.getElementByName( PentahoDefaults.NAMESPACE, elementType, elementName );
        if ( element != null ) {
          metaStore.deleteElement( PentahoDefaults.NAMESPACE, elementType, element.getId() );
          element.setName( newName );
          metaStore.createElement( PentahoDefaults.NAMESPACE, elementType, element );
        }
        return newName;
      } catch ( MetaStoreException e ) {
        new ErrorDialog( shell, BaseMessages.getString( PKG, "TransDialog.ErrorDialog.Label" ),
          BaseMessages.getString( PKG, "TransDialog.RenameServiceErrorDialog.Label", elementName ), e );
      }

    }
    return null;
  }

  protected boolean removeService( Shell shell, IMetaStore metaStore, String elementName ) {
    ShowMessageDialog dialog = new ShowMessageDialog( shell, SWT.ICON_QUESTION | SWT.YES | SWT.NO,
      BaseMessages.getString( PKG, "TransDialog.RemoveServiceDialog.Label" ),
      BaseMessages.getString( PKG, "TransDialog.RemoveServiceDialog.Description", elementName ) );
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
        new ErrorDialog( shell, BaseMessages.getString( PKG, "TransDialog.ErrorDialog.Label" ),
          BaseMessages.getString( PKG, "TransDialog.RemoveServiceErrorDialog.Label", elementName ),
          e );
        return false;
      }

    }
    return false;
  }

  protected String createNewService( Shell shell, IMetaStore metaStore ) {
    EnterStringDialog dialog =
      new EnterStringDialog( shell, "table1", BaseMessages.getString( PKG, "TransDialog.NewServiceDialog.Label" ),
        BaseMessages.getString( PKG, "TransDialog.NewServiceDialog.Description" ) );
    String name = dialog.open();
    if ( name != null ) {

      try {
        IMetaStoreElementType elementType = DataServiceMetaStoreUtil.createDataServiceElementTypeIfNeeded( metaStore );
        IMetaStoreElement element = metaStore.getElementByName( PentahoDefaults.NAMESPACE, elementType, name );
        if ( element != null ) {
          throw new MetaStoreException( "The data service with name '" + name + "' already exists" );
        }
      } catch ( MetaStoreException e ) {
        new ErrorDialog( shell, BaseMessages.getString( PKG, "TransDialog.ErrorDialog.Label" ),
          BaseMessages.getString( PKG, "TransDialog.NewServiceErrorDialog.Label" ), e );
        return null;
      }

      return name;
    } else {
      return null;
    }
  }

  private String[] getDataServiceElementNames( Shell shell, IMetaStore metaStore ) {
    try {
      List<DataServiceMeta> dataServices = DataServiceMeta.getMetaStoreFactory( metaStore ).getElements();
      String[] names = new String[ dataServices.size() ];
      int i = 0;
      for ( DataServiceMeta dataService : dataServices ) {
        names[ i ] = dataService.getName();
        i++;
      }
      return names;
    } catch ( Exception e ) {
      KettleRepositoryLostException krle = KettleRepositoryLostException.lookupStackStrace( e );
      if(krle != null) {
        throw krle;
      }
      e.printStackTrace();
      new ErrorDialog( shell, "Error", "Error getting list of data services", e );
      return new String[]{};
    }
  }

  @Override
  public void getData( TransMeta transMeta ) throws KettleException {
    try {
      // Data service metadata
      //
      DataServiceMeta dataService = DataServiceMetaStoreUtil.fromTransMeta( transMeta, transMeta.getMetaStore() );

      if ( dataService != null ) {
        optimizationList = dataService.getPushDownOptimizationMeta();
        wServiceName.setText( Const.NVL( dataService.getName(), "" ) );
        wServiceStep.setText( getStepname( dataService, transMeta ) );
        refreshOptimizationList();
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unable to load data service", e );
    }
    updateButtons();
  }

  /**
   * Returns the step name associated with the data service if there is a corresponding
   * step in transMeta, otherwise an empty string.
   */
  private String getStepname( DataServiceMeta dataService, TransMeta transMeta ) {
    String stepName = dataService.getStepname();
    if ( Arrays.asList( transMeta.getStepNames() ).contains( stepName ) ) {
      return stepName;
    }
    return "";
  }


  private void refreshOptimizationList() {
    optimizationListTable.clearAll();
    optimizationListTable.removeEmptyRows();
    boolean firstRow = true;

    for ( PushDownOptimizationMeta optMeta : optimizationList ) {
      PushDownType pushDownType = optMeta.getType();
      if ( pushDownType == null ) {
        logger.warn(
          String.format( "Optimization type is missing from '%s'.  Skipping.",
            optMeta.getName() ) );
        continue;
      }

      TableItem item;
      if ( firstRow ) {
        item = optimizationListTable.table.getItem( 0 );
        firstRow = false;
      } else {
        item = new TableItem( optimizationListTable.table, SWT.NONE );
      }
      int colnr = 1;
      item.setText( colnr++, optMeta.getName() );
      item.setText( colnr++, optMeta.getStepName() );
      item.setText( colnr++, pushDownType.getTypeName() );
      item.setText( colnr, optMeta.isEnabled() ? BaseMessages.getString( PKG, "TransDialog.Enabled.Value" )
        : BaseMessages.getString( PKG, "TransDialog.Disabled.Value" ) );
    }
    optimizationListTable.setRowNums();
    editButton.setEnabled( false );
    deleteButton.setEnabled( false );
    adjustTableColumns( optimizationListTable.table );
  }

  private void adjustTableColumns( Table table ) {
    for ( TableColumn column : table.getColumns() ) {
      column.pack();
    }
  }


  public DataServiceMeta getDataServiceMeta() throws KettleException {
    DataServiceMeta dataService = new DataServiceMeta();
    String serviceName = wServiceName.getText().trim();
    String stepService = wServiceStep.getText().trim();
    if ( ( serviceName.isEmpty() && !stepService.isEmpty() ) || ( !serviceName.isEmpty() && stepService.isEmpty() ) ) {
      throw new KettleException( "Required fields are not filled!" );
    }

    // Get data service details...
    //
    dataService.setName( serviceName );
    dataService.setStepname( stepService );

    // Set enabled/disabled on optimizations
    for ( int i = 0; i < optimizationList.size(); i++ ) {
      PushDownOptimizationMeta optMeta = optimizationList.get( i );
      TableItem tableItem = optimizationListTable.table.getItem( i );
      String text = tableItem.getText( ENABLED_COMBO_COLUMN );
      optMeta.setEnabled( text.equals( BaseMessages.getString( PKG, "TransDialog.Enabled.Value" ) ) );
    }

    dataService.setPushDownOptimizationMeta( optimizationList );
    return dataService;
  }

  @Override
  public void ok( TransMeta transMeta ) throws KettleException {

    try {
      DataServiceMetaStoreUtil.toTransMeta( transMeta, transMeta.getMetaStore(), getDataServiceMeta(), true );
      transMeta.setChanged();
    } catch ( Exception e ) {
      throw new KettleException( "Error reading data service metadata", e );
    }

  }

  @Override
  public CTabItem getTab() {
    return wDataServiceTab;
  }

  public List<AutoOptimizationService> getAutoOptimizationServices() {
    return autoOptimizationServices;
  }

  public void setAutoOptimizationServices( List<AutoOptimizationService> autoOptimizationServices ) {
    this.autoOptimizationServices = autoOptimizationServices;
  }
}
