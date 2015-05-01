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

import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.TreeSelection;
import org.pentaho.di.ui.spoon.delegates.SpoonTreeDelegateExtension;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.List;

@ExtensionPoint( id = "DataServiceTreeDelegateExtension", description = "Refreshes data services subtree",
  extensionPointId = "SpoonTreeDelegateExtension" )
public class DataServiceTreeDelegateExtension implements ExtensionPointInterface {

  private static final Class<?> PKG = DataServiceTreeDelegateExtension.class;
  public static final String STRING_DATA_SERVICES =
    BaseMessages.getString( PKG, "DataServicesDialog.STRING_DATA_SERVICES" );

  private DataServiceMetaStoreUtil metaStoreUtil;

  public DataServiceTreeDelegateExtension( DataServiceMetaStoreUtil metaStoreUtil ) {
    this.metaStoreUtil = metaStoreUtil;
  }

  @Override public void callExtensionPoint( LogChannelInterface log, Object extension ) throws KettleException {
    SpoonTreeDelegateExtension treeDelExt = (SpoonTreeDelegateExtension) extension;

    int caseNumber = treeDelExt.getCaseNumber();
    AbstractMeta transMeta = treeDelExt.getTransMeta();
    String[] path = treeDelExt.getPath();
    List<TreeSelection> objects = treeDelExt.getObjects();

    TreeSelection object = null;
    Spoon spoon = Spoon.getInstance();

    switch ( caseNumber ) {
      case 3:
        if ( path[ 2 ].equals( STRING_DATA_SERVICES ) ) {
          object = new TreeSelection( path[ 2 ], DataServiceMeta.class, transMeta );
        }
        break;
      case 4:
        if ( path[ 2 ].equals( STRING_DATA_SERVICES ) ) {
          try {
            DataServiceMeta dataService = extractDataServiceFromMetaStore( path[ 3 ], spoon.getMetaStore() );
            object = new TreeSelection( path[ 3 ], dataService, transMeta );
          } catch ( MetaStoreException e ) {
            // Do Nothing
          }
        }
        break;
    }

    if ( object != null ) {
      objects.add( object );
    }
  }

  private DataServiceMeta extractDataServiceFromMetaStore( String serviceName, IMetaStore metaStore ) throws
    MetaStoreException {
    for ( DataServiceMeta dataServiceMeta : metaStoreUtil.getMetaStoreFactory( metaStore ).getElements() ) {
      if ( serviceName.equals( dataServiceMeta.getName() ) ) {
        return dataServiceMeta;
      }
    }
    return null;
  }
}
