package org.pentaho.di.ui.repository.repositoryexplorer.model;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.Directory;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.services.IAclService;
import org.pentaho.di.ui.repository.repositoryexplorer.AccessDeniedException;
import org.pentaho.di.ui.repository.repositoryexplorer.IAclObject;

public class UIEERepositoryDirectory extends UIRepositoryDirectory implements IAclObject{
  private IAclService aclService;
  public UIEERepositoryDirectory() {
    super();
    // TODO Auto-generated constructor stub
  }

  public UIEERepositoryDirectory(Directory rd, Repository rep) {
    super(rd, rep);
    initializeService(rep);
  }

  public UIEERepositoryDirectory(Directory rd, UIRepositoryDirectory uiParent, Repository rep) {
    super(rd, uiParent, rep);
    initializeService(rep);
  }
  public void getAcls(UIRepositoryObjectAcls acls, boolean forceParentInheriting) throws AccessDeniedException{
    try {
      acls.setObjectAcl(aclService.getAcl(getObjectId(), forceParentInheriting));
    } catch(KettleException ke) {
      throw new AccessDeniedException(ke);
    }
  }

  public void getAcls(UIRepositoryObjectAcls acls) throws AccessDeniedException{
    try {
      acls.setObjectAcl(aclService.getAcl(getObjectId(), false));
    } catch(KettleException ke) {
      throw new AccessDeniedException(ke);
    }
  }

  public void setAcls(UIRepositoryObjectAcls security) throws AccessDeniedException{
    try {
      aclService.setAcl(getObjectId(), security.getObjectAcl());
    } catch (KettleException e) {
      throw new AccessDeniedException(e);
    }
  }
  
  private void initializeService(Repository rep) {
    try {
      if (rep.hasService(IAclService.class)) {
        aclService = (IAclService) rep.getService(IAclService.class);
      } else {
        throw new IllegalStateException();
      }
    } catch (KettleException e) {
      throw new RuntimeException(e);
    } 

  }
}
