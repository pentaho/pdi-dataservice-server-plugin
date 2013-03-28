/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.repository.pur.model;

import org.pentaho.di.trans.TransMeta;

public class EETransMeta extends TransMeta implements ILockable, java.io.Serializable {

  private static final long serialVersionUID = -5959504570945456271L; /* EESOURCE: UPDATE SERIALVERUID */
  private RepositoryLock repositoryLock;

  /**
   * @return the repositoryLock
   */
  public RepositoryLock getRepositoryLock() {
    return repositoryLock;
  }

  /**
   * @param repositoryLock the repositoryLock to set
   */
  public void setRepositoryLock(RepositoryLock repositoryLock) {
    this.repositoryLock = repositoryLock;
  }
}
