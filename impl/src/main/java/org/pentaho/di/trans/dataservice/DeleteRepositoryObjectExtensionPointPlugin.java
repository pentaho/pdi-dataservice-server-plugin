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


package org.pentaho.di.trans.dataservice;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.ui.repository.RepositoryExtension;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryDirectory;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryObject;

@ExtensionPoint(
  id = "DeleteRepositoryObjectExtensionPointPlugin",
  extensionPointId = "AfterDeleteRepositoryObject",
  description = "Remove a data service associated with a deleted repository object" )
public class DeleteRepositoryObjectExtensionPointPlugin implements ExtensionPointInterface {
  private final DataServiceMetaStoreUtil metaStoreUtil;

  public DeleteRepositoryObjectExtensionPointPlugin( DataServiceContext context ) {
    this.metaStoreUtil = context.getMetaStoreUtil();
  }

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    RepositoryExtension repositoryExtension = (RepositoryExtension) object;
    UIRepositoryObject repositoryObject = repositoryExtension.getRepositoryObject();
    //if a directory get all transformations recursively
    if ( repositoryObject instanceof UIRepositoryDirectory ) {
      List<UIRepositoryObject> transformationList = new ArrayList<UIRepositoryObject>();
      getAllTransformations( (UIRepositoryDirectory) repositoryObject, transformationList );
      for ( UIRepositoryObject uiRepositoryObject : transformationList ) {
        Repository repository = uiRepositoryObject.getRepository();
        metaStoreUtil.clearReferences( repository.loadTransformation( uiRepositoryObject.getObjectId(), null ) );
      }
    } else if ( repositoryObject.getRepositoryElementType().equals( RepositoryObjectType.TRANSFORMATION ) ) {
      Repository repository = repositoryObject.getRepository();
      metaStoreUtil.clearReferences( repository.loadTransformation( repositoryObject.getObjectId(), null ) );
    }
  }

  private static void getAllTransformations( UIRepositoryDirectory repositoryObject, List<UIRepositoryObject> objectsTransformations ) throws KettleException {
    //test if has sub-directories
    if ( repositoryObject.getChildren() != null && repositoryObject.getChildren().size() > 0 ) {
      for ( UIRepositoryObject subDirectory : repositoryObject.getChildren() ) {
        if ( subDirectory instanceof UIRepositoryDirectory ) {
          getAllTransformations( (UIRepositoryDirectory) subDirectory, objectsTransformations );
        }
      }
    }
    //getting all the transformations
    for ( UIRepositoryObject uiRepositoryObject : repositoryObject.getRepositoryObjects() ) {
      if ( RepositoryObjectType.TRANSFORMATION.equals( uiRepositoryObject.getRepositoryElementType() ) ) {
        objectsTransformations.add( uiRepositoryObject );
      }
    }
  }

}
