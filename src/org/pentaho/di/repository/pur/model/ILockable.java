/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.repository.pur.model;

import org.pentaho.di.core.exception.KettleException;

public interface ILockable {
  public RepositoryLock getRepositoryLock() throws KettleException;
  public void setRepositoryLock(RepositoryLock lock) throws KettleException;
}
