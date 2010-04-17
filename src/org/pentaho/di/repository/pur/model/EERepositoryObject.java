package org.pentaho.di.repository.pur.model;

import java.util.Date;

import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryObject;
import org.pentaho.di.repository.RepositoryObjectType;

public class EERepositoryObject extends RepositoryObject implements ILockObject{
  private String lockMessage;
  
  
  public EERepositoryObject() {
    super();
    // TODO Auto-generated constructor stub
  }

  public EERepositoryObject(ObjectId objectId, String name, RepositoryDirectoryInterface repositoryDirectory,
      String modifiedUser, Date modifiedDate, RepositoryObjectType objectType, String description, boolean deleted) {
    super(objectId, name, repositoryDirectory, modifiedUser, modifiedDate, objectType, description, deleted);
    // TODO Auto-generated constructor stub
  }

  public EERepositoryObject(ObjectId objectId, String name, RepositoryDirectoryInterface repositoryDirectory,
      String modifiedUser, Date modifiedDate, RepositoryObjectType objectType, String description, String lockMessage, boolean deleted) {
    super(objectId, name, repositoryDirectory, modifiedUser, modifiedDate, objectType, description, deleted);
    this.lockMessage = lockMessage;
  }
  
  /**
   * @return the lockMessage
   */
  public String getLockMessage() {
    return lockMessage;
  }

  /**
   * @param lockMessage the lockMessage to set
   */
  public void setLockMessage(String lockMessage) {
    this.lockMessage = lockMessage;
  }
}
