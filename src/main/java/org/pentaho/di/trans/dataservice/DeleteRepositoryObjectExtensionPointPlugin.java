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
