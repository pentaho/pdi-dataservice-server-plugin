package org.pentaho.di.repository.pur.model;

public interface ILockObject {

  /**
   * @return is this object locked?
   */
  public boolean isLocked();
  
  /**
   * @return the lockMessage
   */
  public String getLockMessage();

  /**
   * @return the repository lock for this object
   */
  public RepositoryLock getLock();
  
  /**
   * Set the lock for this object
   */
  public void setLock(RepositoryLock lock);
}
