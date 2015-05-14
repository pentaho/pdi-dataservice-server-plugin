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
import com.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Tree;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.TreeSelection;

import java.util.List;

@ExtensionPoint( id = "DataServicePopupMenuExtension", description = "Creates popup menus for data services",
  extensionPointId = "SpoonPopupMenuExtension" )
public class DataServicePopupMenuExtension implements ExtensionPointInterface {

  private static final Class<?> PKG = DataServicePopupMenuExtension.class;
  private static final Log logger = LogFactory.getLog( DataServicePopupMenuExtension.class );
  private DataServiceDelegate delegate;

  public DataServiceMeta selectedDataService;

  public DataServicePopupMenuExtension( DataServiceMetaStoreUtil metaStoreUtil,
                                        List<AutoOptimizationService> autoOptimizationServices,
                                        List<PushDownFactory> pushDownFactories ) {
    delegate = new DataServiceDelegate( metaStoreUtil, autoOptimizationServices, pushDownFactories );
  }

  @Override public void callExtensionPoint( LogChannelInterface log, Object extension ) throws KettleException {

    Menu popupMenu = null;

    Tree selectionTree = (Tree) extension;
    TreeSelection[] objects = getSpoon().getTreeObjects( selectionTree );
    TreeSelection object = objects[ 0 ];
    Object selection = object.getSelection();

    if ( selection instanceof Class<?> && selection.equals( DataServiceMeta.class ) ) {
      popupMenu = new Menu( selectionTree );
      createRootPopupMenu( popupMenu );
    } else if ( selection instanceof DataServiceMeta ) {
      selectedDataService = (DataServiceMeta) selection;
      popupMenu = new Menu( selectionTree );
      createItemPopupMenu( popupMenu );
    }

    if ( popupMenu != null ) {
      ConstUI.displayMenu( popupMenu, selectionTree );
    } else {
      selectionTree.setMenu( null );
    }
  }

  private void createRootPopupMenu( Menu parent ) {
    createPopupMenu( parent, BaseMessages.getString( PKG, "DataServicePopupMenu.New.Label" ),
      new DataServiceNewCommand() );
  }

  private void createItemPopupMenu( Menu parent ) {
    createPopupMenu( parent, BaseMessages.getString( PKG, "DataServicePopupMenu.New.Label" ),
      new DataServiceNewCommand() );
    createPopupMenu( parent, BaseMessages.getString( PKG, "DataServicePopupMenu.Edit.Label" ),
      new DataServiceEditCommand() );
    createPopupMenu( parent, BaseMessages.getString( PKG, "DataServicePopupMenu.Delete.Label" ),
      new DataServiceDeleteCommand() );
    new MenuItem( parent, SWT.SEPARATOR );
    createPopupMenu( parent, BaseMessages.getString( PKG, "DataServicePopupMenu.Test.Label" ),
      new DataServiceTestCommand() );
    createPopupMenu( parent, BaseMessages.getString( PKG, "DataServicePopupMenu.ViewTransformation.Label" ),
      new OpenTransformationCommand() );
  }

  private void createPopupMenu( Menu parent, final String label, final DataServiceCommand dataServiceCommand ) {
    MenuItem menuItem = new MenuItem( parent, SWT.NONE );
    menuItem.setText( label );
    menuItem.addSelectionListener( new SelectionListener() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        dataServiceCommand.execute();
      }

      @Override public void widgetDefaultSelected( SelectionEvent selectionEvent ) {

      }
    } );
  }

  interface DataServiceCommand {
    public void execute();
  }

  class DataServiceNewCommand implements DataServiceCommand {
    @Override public void execute() {
      delegate.createNewDataService();
    }
  }

  class DataServiceEditCommand implements DataServiceCommand {
    @Override public void execute() {
      delegate.editDataService( selectedDataService );
    }
  }

  class DataServiceDeleteCommand implements DataServiceCommand {
    @Override public void execute() {
      try {
        TransMeta transMeta = selectedDataService.lookupTransMeta( getRepository() );
        delegate.removeDataService( transMeta, selectedDataService );
      } catch ( KettleException e ) {
        logger.error( "Unable to open transformation", e );
      }
    }
  }

  class DataServiceTestCommand implements DataServiceCommand {
    @Override public void execute() {
      delegate.testDataService( selectedDataService );
    }
  }

  class OpenTransformationCommand implements DataServiceCommand {
    @Override public void execute() {
      try {
        TransMeta transMeta = selectedDataService.lookupTransMeta( getRepository() );
        delegate.openTrans( transMeta );
      } catch ( KettleException e ) {
        logger.error( "Unable to open transformation", e );
      }
    }
  }

  private Repository getRepository() { return getSpoon().getRepository(); }

  private Spoon getSpoon() {
    return Spoon.getInstance();
  }
}
