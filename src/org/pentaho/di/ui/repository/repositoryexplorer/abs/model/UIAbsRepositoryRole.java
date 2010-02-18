package org.pentaho.di.ui.repository.repositoryexplorer.abs.model;

import java.util.List;

import org.pentaho.di.repository.ILogicalRole;
import org.pentaho.di.repository.IRole;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryRole;

public class UIAbsRepositoryRole extends UIRepositoryRole implements ILogicalRole{
 
  public UIAbsRepositoryRole() {
    super();
  }
  
  public UIAbsRepositoryRole(IRole rri) {
    super(rri);
  }
  public List<String> getLogicalRoles() {
    if(this.getRole() instanceof ILogicalRole) {
      return ((ILogicalRole) this.getRole()).getLogicalRoles();
    }
    return null;
  }

  public void setLogicalRoles(List<String> logicalRoles) {
    if(this.getRole() instanceof ILogicalRole) {
      ((ILogicalRole) this.getRole()).setLogicalRoles(logicalRoles);
    }
  }

  public void addLogicalRole(String logicalRole) {
    if(getRole() instanceof ILogicalRole) {
      if(!containsLogicalRole(logicalRole)) {
        ((ILogicalRole) this.getRole()).addLogicalRole(logicalRole);
      }
    }
  }

  public void removeLogicalRole(String logicalRole) {
    if(getRole() instanceof ILogicalRole) {
      if(containsLogicalRole(logicalRole)) {
        ((ILogicalRole) this.getRole()).removeLogicalRole(logicalRole);
      }
    }
  }

  public boolean containsLogicalRole(String logicalRole) {
    if(getRole() instanceof ILogicalRole) {
      return ((ILogicalRole) this.getRole()).containsLogicalRole(logicalRole);
    }
    return false;
  }
}
