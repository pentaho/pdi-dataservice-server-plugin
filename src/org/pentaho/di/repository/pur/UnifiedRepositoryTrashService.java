/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
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

package org.pentaho.di.repository.pur;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.ui.repository.pur.services.ITrashService;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;

public class UnifiedRepositoryTrashService implements ITrashService {
  private final IUnifiedRepository pur;
  private final RootRef rootRef;

  public UnifiedRepositoryTrashService( IUnifiedRepository pur, RootRef rootRef ) {
    this.pur = pur;
    this.rootRef = rootRef;
  }

  @Override
  public void delete( final List<ObjectId> ids ) throws KettleException {
    for ( ObjectId id : ids ) {
      pur.deleteFile( id.getId(), true, null );
    }
    rootRef.clearRef();
  }

  @Override
  public void undelete( final List<ObjectId> ids ) throws KettleException {
    for ( ObjectId id : ids ) {
      pur.undeleteFile( id.getId(), null );
    }
    rootRef.clearRef();
  }

  @Override
  public List<IDeletedObject> getTrash() throws KettleException {
    List<IDeletedObject> trash = new ArrayList<IDeletedObject>();
    List<RepositoryFile> deletedChildren = pur.getDeletedFiles();

    for ( final RepositoryFile file : deletedChildren ) {
      trash.add( new IDeletedObject() {

        @Override
        public String getOriginalParentPath() {
          return file.getOriginalParentFolderPath();
        }

        @Override
        public Date getDeletedDate() {
          return file.getDeletedDate();
        }

        @Override
        public String getType() {
          if ( file.getName().endsWith( RepositoryObjectType.TRANSFORMATION.getExtension() ) ) {
            return RepositoryObjectType.TRANSFORMATION.name();
          } else if ( file.getName().endsWith( RepositoryObjectType.JOB.getExtension() ) ) {
            return RepositoryObjectType.JOB.name();
          } else {
            return null;
          }
        }

        @Override
        public ObjectId getId() {
          return new StringObjectId( file.getId().toString() );
        }

        @Override
        public String getName() {
          return file.getTitle();
        }

      } );
    }

    return trash;
  }
}
