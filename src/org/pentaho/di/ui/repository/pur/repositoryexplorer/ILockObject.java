/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.ui.repository.pur.repositoryexplorer;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.pur.model.RepositoryLock;

public interface ILockObject {
  public String getLockMessage() throws KettleException;
  public void lock(String lockNote) throws KettleException;
  public void unlock() throws KettleException;
  public RepositoryLock getRepositoryLock() throws KettleException;
  public boolean isLocked() throws KettleException;
}
