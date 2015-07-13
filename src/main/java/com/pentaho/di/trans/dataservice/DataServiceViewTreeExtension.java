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
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.TreeItem;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.spoon.SelectionTreeExtension;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.List;

@ExtensionPoint( id = "DataServiceViewTreeExtension", description = "Refreshes data services subtree",
  extensionPointId = "SpoonViewTreeExtension" )
public class DataServiceViewTreeExtension implements ExtensionPointInterface {

  private DataServiceDelegate delegate;

  private DataServiceMetaStoreUtil metaStoreUtil;
  private static final Class<?> PKG = DataServiceTreeDelegateExtension.class;
  public static final String STRING_DATA_SERVICES =
    BaseMessages.getString( PKG, "DataServicesDialog.STRING_DATA_SERVICES" );

  public DataServiceViewTreeExtension( DataServiceMetaStoreUtil metaStoreUtil,
                                       List<AutoOptimizationService> autoOptimizationServices,
                                       List<PushDownFactory> pushDownFactories ) {
    this.metaStoreUtil = metaStoreUtil;
    delegate = new DataServiceDelegate( metaStoreUtil, autoOptimizationServices, pushDownFactories );
  }

  @Override public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    SelectionTreeExtension selectionTreeExtension = (SelectionTreeExtension) object;
    if ( selectionTreeExtension.getAction().equals( Spoon.REFRESH_SELECTION_EXTENSION ) ) {
      refreshTree( selectionTreeExtension );
    }
    if ( selectionTreeExtension.getAction().equals( Spoon.EDIT_SELECTION_EXTENSION ) ) {
      editDataService( selectionTreeExtension );
    }
  }

  private void refreshTree( SelectionTreeExtension selectionTreeExtension ) {

    TreeItem tiRootName = selectionTreeExtension.getTiRootName();
    GUIResource guiResource = selectionTreeExtension.getGuiResource();

    TreeItem tiDSTitle = createTreeItem( tiRootName, STRING_DATA_SERVICES, guiResource.getImageFolder() );

    try {
      List<DataServiceMeta> dataServices = metaStoreUtil.getMetaStoreFactory( getSpoon().getMetaStore() ).getElements();
      for ( DataServiceMeta dataService : dataServices ) {
        createTreeItem( tiDSTitle, dataService.getName(), guiResource
          .getImage( "images/data-services_padding.svg", getClass().getClassLoader(), ConstUI.MEDIUM_ICON_SIZE,
            ConstUI.MEDIUM_ICON_SIZE ) );
      }
    } catch ( MetaStoreException e ) {
      // Do Nothing
    }
  }

  private void editDataService( SelectionTreeExtension selectionTreeExtension ) {
    Object selection = selectionTreeExtension.getSelection();
    if ( selection instanceof DataServiceMeta ) {
      delegate.editDataService( (DataServiceMeta) selection );
    }
  }

  private TreeItem createTreeItem( TreeItem parent, String text, Image image ) {
    TreeItem item = new TreeItem( parent, SWT.NONE );
    item.setText( text );
    item.setImage( image );
    return item;
  }

  private Spoon getSpoon() {
    return Spoon.getInstance();
  }
}
