/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.di.trans.dataservice.ui.menu;

import org.eclipse.swt.graphics.Image;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.tree.TreeNode;
import org.pentaho.di.ui.spoon.tree.TreeFolderProvider;

/**
 * Created by bmorrise on 7/6/18.
 */
public class DataServiceFolderProvider extends TreeFolderProvider {

  private static final Class<?> PKG = DataServiceTreeDelegateExtension.class;
  public static final String STRING_DATA_SERVICES =
          BaseMessages.getString( PKG, "DataServicePopupMenu.TITLE" );

  private DataServiceDelegate dataServiceDelegate;

  public DataServiceFolderProvider( DataServiceDelegate dataServiceDelegate ) {
    this.dataServiceDelegate = dataServiceDelegate;
  }

  @Override
  public void refresh( AbstractMeta meta, TreeNode treeNode, String filter ) {
    for ( DataServiceMeta dataService : dataServiceDelegate.getDataServices( (TransMeta) meta ) ) {
      if ( !filterMatch( dataService.getName(), filter ) ) {
        continue;
      }
      createTreeNode( treeNode, dataService.getName(), getDataServiceImage( GUIResource.getInstance(), dataService ) );
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

  @Override
  public String getTitle() {
    return STRING_DATA_SERVICES;
  }
}
