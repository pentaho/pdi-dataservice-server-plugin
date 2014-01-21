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

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.repository.pur.model.RepositoryLock;
import org.pentaho.di.ui.repository.pur.services.ILockService;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;

public class UnifiedRepositoryLockService implements ILockService {
  private final IUnifiedRepository pur;

  public UnifiedRepositoryLockService( IUnifiedRepository pur ) {
    this.pur = pur;
  }

  protected void lockFileById( final ObjectId id, final String message ) throws KettleException {
    pur.lockFile( id.getId(), message );
  }

  public RepositoryLock getLock( final RepositoryFile file ) throws KettleException {
    if ( file.isLocked() ) {
      return new RepositoryLock( new StringObjectId( file.getId().toString() ), file.getLockMessage(), file
          .getLockOwner(), file.getLockOwner(), file.getLockDate() );
    } else {
      return null;
    }
  }

  protected RepositoryLock getLockById( final ObjectId id ) throws KettleException {
    try {
      RepositoryFile file = pur.getFileById( id.getId() );
      return getLock( file );
    } catch ( Exception e ) {
      throw new KettleException( "Unable to get lock for object with id [" + id + "]", e );
    }
  }

  protected void unlockFileById( ObjectId id ) throws KettleException {
    pur.unlockFile( id.getId() );
  }

  @Override
  public RepositoryLock lockJob( final ObjectId idJob, final String message ) throws KettleException {
    lockFileById( idJob, message );
    return getLockById( idJob );
  }

  @Override
  public void unlockJob( ObjectId idJob ) throws KettleException {
    unlockFileById( idJob );
  }

  @Override
  public RepositoryLock getJobLock( ObjectId idJob ) throws KettleException {
    return getLockById( idJob );
  }

  @Override
  public RepositoryLock lockTransformation( final ObjectId idTransformation, final String message )
    throws KettleException {
    lockFileById( idTransformation, message );
    return getLockById( idTransformation );
  }

  @Override
  public void unlockTransformation( ObjectId idTransformation ) throws KettleException {
    unlockFileById( idTransformation );
  }

  @Override
  public RepositoryLock getTransformationLock( ObjectId idTransformation ) throws KettleException {
    return getLockById( idTransformation );
  }

  @Override
  public boolean canUnlockFileById( final ObjectId id ) {
    return pur.canUnlockFile( id.getId() );
  }

}
