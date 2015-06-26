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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.ui.repository.RepositoryExtension;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryObject;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.List;

@ExtensionPoint(
  id = "DeleteRepositoryObjectExtensionPointPlugin",
  extensionPointId = "AfterDeleteRepositoryObject",
  description = "Remove a data service associated with a deleted repository object"
)
public class DeleteRepositoryObjectExtensionPointPlugin implements ExtensionPointInterface {

  private static final Log logger = LogFactory.getLog( DeleteRepositoryObjectExtensionPointPlugin.class );

  private DataServiceMetaStoreUtil metaStoreUtil;

  public DeleteRepositoryObjectExtensionPointPlugin( DataServiceContext context ) {
    this.metaStoreUtil = context.getMetaStoreUtil();
  }

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    RepositoryExtension repositoryExtension = (RepositoryExtension) object;
    UIRepositoryObject repositoryObject = repositoryExtension.getRepositoryObject();
    if ( repositoryObject.getRepositoryElementType().equals( RepositoryObjectType.TRANSFORMATION ) ) {
      TransMeta transMeta = getSpoon().getRepository().loadTransformation( repositoryObject.getObjectId(), null );

      List<DataServiceMeta> dataServices;
      try {
        dataServices = metaStoreUtil.getDataServices( transMeta );
      } catch ( MetaStoreException e ) {
        logger.error( "Unable to delete Data Service", e );
        return;
      }

      for ( DataServiceMeta dataService : dataServices ) {
        try {
          metaStoreUtil.removeDataService( transMeta, getSpoon().getMetaStore(), dataService );
        } catch ( Exception e ) {
          logger.error( "Unable to delete Data Service", e );
        }
      }
    }
  }

  private Spoon getSpoon() {
    return Spoon.getInstance();
  }

}
