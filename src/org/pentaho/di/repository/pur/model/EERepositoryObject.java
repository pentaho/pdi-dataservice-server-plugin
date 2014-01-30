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

package org.pentaho.di.repository.pur.model;

import java.util.Date;

import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryObject;
import org.pentaho.di.repository.RepositoryObjectType;

public class EERepositoryObject extends RepositoryObject implements ILockObject, java.io.Serializable {

  private static final long serialVersionUID = -566113926064789982L; /* EESOURCE: UPDATE SERIALVERUID */
  private String lockMessage;
  private RepositoryLock lock;
  
  public EERepositoryObject() {
    super();
    // TODO Auto-generated constructor stub
  }

  public EERepositoryObject(ObjectId objectId, String name, RepositoryDirectoryInterface repositoryDirectory,
      String modifiedUser, Date modifiedDate, RepositoryObjectType objectType, String description, RepositoryLock lock, boolean deleted) {
    super(objectId, name, repositoryDirectory, modifiedUser, modifiedDate, objectType, description, deleted);
    setLock(lock);
  }

  public boolean isLocked() {
    return lock != null;
  }

  /**
   * @return the lockMessage
   */
  public String getLockMessage() {
    return lockMessage;
  }

  public RepositoryLock getLock() {
    return lock;
  }

  public void setLock(RepositoryLock lock) {
    this.lock = lock;
    lockMessage = lock == null ? null : lock.getMessage() + " (" + lock.getLogin() + " since " //$NON-NLS-1$ //$NON-NLS-2$
        + XMLHandler.date2string(lock.getLockDate()) + ")"; //$NON-NLS-1$
  }
}
