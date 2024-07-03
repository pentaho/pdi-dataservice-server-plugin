/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

import java.util.Optional;

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
  public void refresh( Optional<AbstractMeta> meta, TreeNode treeNode, String filter ) {
    if ( meta.isPresent() ) {
      for ( DataServiceMeta dataService : dataServiceDelegate.getDataServices( (TransMeta) meta.get() ) ) {
        if ( !filterMatch( dataService.getName(), filter ) ) {
          continue;
        }
        createTreeNode( treeNode, dataService.getName(), getDataServiceImage( GUIResource.getInstance(), dataService ) );
      }
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
