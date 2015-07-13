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

package org.pentaho.di.trans.dataservice;

import org.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
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
