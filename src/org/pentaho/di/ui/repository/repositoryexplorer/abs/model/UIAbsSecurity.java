package org.pentaho.di.ui.repository.repositoryexplorer.abs.model;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.IRole;
import org.pentaho.di.repository.RepositorySecurityManager;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.di.ui.repository.repositoryexplorer.model.IUIRole;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryUser;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UISecurity;

public class UIAbsSecurity extends UISecurity {

  public UIAbsSecurity() {
    super();
  }
  public UIAbsSecurity(RepositorySecurityManager rsm) {
    try  {
      if(rsm != null && rsm.getUsers() != null) {
        for(UserInfo user:rsm.getUsers()) {
          userList.add(new UIRepositoryUser(user));
        }
        this.firePropertyChange("userList", null, userList); //$NON-NLS-1$
        for(IRole role:rsm.getRoles()) {
          roleList.add(new UIAbsRepositoryRole(role));
          
        }
        this.firePropertyChange("roleList", null, roleList); //$NON-NLS-1$
      }
    } catch(KettleException ke) {
      
    }
  }
  
  public void addLogicalRole(String logicalRole) {
    IUIRole role = getSelectedRole();
    ((UIAbsRepositoryRole) role).addLogicalRole(logicalRole);
  }

  public void removeLogicalRole(String logicalRole) {
    IUIRole role = getSelectedRole();
    ((UIAbsRepositoryRole) role).removeLogicalRole(logicalRole);
  }

}
