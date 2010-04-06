package org.pentaho.di.ui.repository.repositoryexplorer;

import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryObjectAcls;

public interface IAclObject {

  public void getAcls(UIRepositoryObjectAcls acls) throws AccessDeniedException;

  public void getAcls(UIRepositoryObjectAcls acls, boolean forceParentInheriting) throws AccessDeniedException;

  public void setAcls(UIRepositoryObjectAcls security) throws AccessDeniedException;
}
