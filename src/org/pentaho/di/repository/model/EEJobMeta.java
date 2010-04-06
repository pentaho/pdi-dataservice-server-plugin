package org.pentaho.di.repository.model;

import org.pentaho.di.job.JobMeta;

public class EEJobMeta extends JobMeta implements ILockable{
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
