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

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.ui.spoon.TreeSelection;
import org.pentaho.di.ui.spoon.delegates.SpoonTreeDelegateExtension;

import java.util.List;

@ExtensionPoint( id = "DataServiceTreeDelegateExtension", description = "Refreshes data services subtree",
  extensionPointId = "SpoonTreeDelegateExtension" )
public class DataServiceTreeDelegateExtension implements ExtensionPointInterface {

  private static final Class<?> PKG = DataServiceTreeDelegateExtension.class;
  public static final String STRING_DATA_SERVICES =
    BaseMessages.getString( PKG, "DataServicePopupMenu.TITLE" );

  private DataServiceMetaStoreUtil metaStoreUtil;

  public DataServiceTreeDelegateExtension( DataServiceContext context ) {
    this.metaStoreUtil = context.getMetaStoreUtil();
  }

  @Override public void callExtensionPoint( LogChannelInterface log, Object extension ) throws KettleException {
    SpoonTreeDelegateExtension treeDelExt = (SpoonTreeDelegateExtension) extension;

    int caseNumber = treeDelExt.getCaseNumber();
    if ( !( treeDelExt.getTransMeta() instanceof TransMeta ) ) {
      return;
    }
    TransMeta transMeta = (TransMeta) treeDelExt.getTransMeta();
    String[] path = treeDelExt.getPath();
    List<TreeSelection> objects = treeDelExt.getObjects();

    TreeSelection object = null;

    if ( path[2].equals( STRING_DATA_SERVICES ) ) {
      switch ( caseNumber ) {
        case 3:
          object = new TreeSelection( path[2], DataServiceMeta.class, transMeta );
          break;
        case 4:
          try {
            DataServiceMeta dataService = metaStoreUtil.getDataService( path[3], transMeta );
            object = new TreeSelection( path[ 3 ], dataService, transMeta );
          } catch ( Exception e ) {
            // Do Nothing
          }
          break;
      }
    }

    if ( object != null ) {
      objects.add( object );
    }
  }
}
