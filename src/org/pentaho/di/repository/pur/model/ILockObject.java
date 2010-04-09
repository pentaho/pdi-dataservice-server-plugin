package org.pentaho.di.repository.pur.model;

public interface ILockObject {

  /**
   * @return the lockMessage
   */
  public String getLockMessage();

  /**
   * @param lockMessage the lockMessage to set
   */
  public void setLockMessage(String lockMessage);
}
