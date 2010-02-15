package org.pentaho.di.ui.repository.repositoryexplorer.abs.model;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.IRole;
import org.pentaho.di.repository.RepositoryUserInterface;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryRole;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryUser;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UISecurity;

public class UIAbsSecurity extends UISecurity {

  public UIAbsSecurity() {
    super();
  }
  public UIAbsSecurity(RepositoryUserInterface rui) {
    try  {
      if(rui != null && rui.getUsers() != null) {
        for(UserInfo user:rui.getUsers()) {
          userList.add(new UIRepositoryUser(user));
        }
        this.firePropertyChange("userList", null, userList); //$NON-NLS-1$
        for(IRole role:rui.getRoles()) {
          roleList.add(new UIAbsRepositoryRole(role));
          
        }
        this.firePropertyChange("roleList", null, roleList); //$NON-NLS-1$
      }
    } catch(KettleException ke) {
      
    }
  }
  
  public UIRepositoryRole getSelectedRole() {
    return selectedRole;
  }
  public void setSelectedRole(UIRepositoryRole selectedRole) {
    this.selectedRole = new UIAbsRepositoryRole(selectedRole.getRole());
    this.firePropertyChange("selectedRole", null, selectedRole); //$NON-NLS-1$
    setSelectedRoleIndex(getIndexOfRole(selectedRole));       
  }
  
  public void addLogicalRole(String logicalRole) {
    ((UIAbsRepositoryRole) getSelectedRole()).addLogicalRole(logicalRole);
  }

  public void removeLogicalRole(String logicalRole) {
    ((UIAbsRepositoryRole) getSelectedRole()).removeLogicalRole(logicalRole);
  }

}
