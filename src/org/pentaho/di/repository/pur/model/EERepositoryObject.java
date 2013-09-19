/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
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
