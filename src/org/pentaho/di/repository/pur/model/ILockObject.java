/*!
* The Pentaho proprietary code is licensed under the terms and conditions
* of the software license agreement entered into between the entity licensing
* such code and Pentaho Corporation.
*
* This software costs money - it is not free
*
* Copyright 2002 - 2013 Pentaho Corporation.  All rights reserved.
*/

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
