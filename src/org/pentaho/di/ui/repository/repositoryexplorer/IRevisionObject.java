package org.pentaho.di.ui.repository.repositoryexplorer;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryObjectRevision;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryObjectRevisions;

public interface IRevisionObject {
  public UIRepositoryObjectRevisions getRevisions() throws KettleException;
  public void restoreRevision(UIRepositoryObjectRevision revision, String commitMessage) throws KettleException;
}
