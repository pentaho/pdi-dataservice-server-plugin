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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.repository.RepositoryExtension;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UITransformation;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

@ExtensionPoint(
  id = "DeleteRepositoryObjectExtensionPointPlugin",
  extensionPointId = "AfterDeleteRepositoryObject",
  description = "Remove a data service associated with a deleted repository object"
)
public class DeleteRepositoryObjectExtensionPointPlugin implements ExtensionPointInterface {

  private static final Log logger = LogFactory.getLog( DeleteRepositoryObjectExtensionPointPlugin.class );

  private DataServiceMetaStoreUtil metaStoreUtil;

  public DeleteRepositoryObjectExtensionPointPlugin( DataServiceMetaStoreUtil metaStoreUtil ) {
    this.metaStoreUtil = metaStoreUtil;
  }

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    RepositoryExtension repositoryExtension = (RepositoryExtension) object;
    if ( repositoryExtension.getRepositoryObject() instanceof UITransformation ) {
      UITransformation transformation = (UITransformation) repositoryExtension.getRepositoryObject();
      TransMeta transMeta = getSpoon().getRepository().loadTransformation( transformation.getObjectId(), null );
      for ( StepMeta stepMeta : transMeta.getSteps() ) {
        try {
          DataServiceMeta dataService =
            metaStoreUtil.fromTransMeta( transMeta, getSpoon().getMetaStore(), stepMeta.getName() );
          metaStoreUtil.getMetaStoreFactory( getSpoon().getMetaStore() ).deleteElement( dataService.getName() );
          getSpoon().refreshTree();
        } catch ( MetaStoreException e ) {
          logger.error( "Unable to load Data Service", e );
        }
      }
    }
  }

  private Spoon getSpoon() {
    return Spoon.getInstance();
  }

}
