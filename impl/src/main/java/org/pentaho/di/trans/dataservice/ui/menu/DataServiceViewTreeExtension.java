/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.dataservice.ui.menu;

import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.TreeItem;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.spoon.SelectionTreeExtension;
import org.pentaho.di.ui.spoon.Spoon;

@ExtensionPoint( id = "DataServiceViewTreeExtension", description = "Refreshes data services subtree",
  extensionPointId = "SpoonViewTreeExtension" )
public class DataServiceViewTreeExtension implements ExtensionPointInterface {

  private DataServiceDelegate delegate;

  private static final Class<?> PKG = DataServiceTreeDelegateExtension.class;
  public static final String STRING_DATA_SERVICES =
    BaseMessages.getString( PKG, "DataServicePopupMenu.TITLE" );

  public DataServiceViewTreeExtension( DataServiceContext context ) {
    delegate = context.getDataServiceDelegate();
  }

  @Override public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    SelectionTreeExtension selectionTreeExtension = (SelectionTreeExtension) object;
    if ( selectionTreeExtension.getAction().equals( Spoon.REFRESH_SELECTION_EXTENSION ) ) {
      if ( selectionTreeExtension.getMeta() instanceof TransMeta ) {
        refreshTree( selectionTreeExtension );
      }
    }
    if ( selectionTreeExtension.getAction().equals( Spoon.EDIT_SELECTION_EXTENSION ) ) {
      editDataService( selectionTreeExtension );
    }
  }

  protected void refreshTree( SelectionTreeExtension selectionTreeExtension ) {
    TransMeta meta = (TransMeta) selectionTreeExtension.getMeta();

    TreeItem tiRootName = selectionTreeExtension.getTiRootName();
    GUIResource guiResource = selectionTreeExtension.getGuiResource();

    TreeItem tiDSTitle = createTreeItem( tiRootName, STRING_DATA_SERVICES, guiResource.getImageFolder() );

    for ( DataServiceMeta dataService : delegate.getDataServices( meta ) ) {
      createTreeItem( tiDSTitle, dataService.getName(), getDataServiceImage( guiResource, dataService ) );
    }
  }

  private Image getDataServiceImage( GUIResource guiResource, DataServiceMeta dataService ) {
    String image = dataService.isStreaming()
        ? "images/data-services-streaming_padding.svg"
        : "images/data-services_padding.svg";
    return guiResource.getImage(
        image, getClass().getClassLoader(), ConstUI.MEDIUM_ICON_SIZE,
        ConstUI.MEDIUM_ICON_SIZE );
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

}
