package org.pentaho.di.ui.repository.repositoryexplorer.model;

import java.util.HashSet;
import java.util.Set;

import org.pentaho.di.repository.IEEUser;
import org.pentaho.di.repository.IRole;
import org.pentaho.di.repository.IUser;
import org.pentaho.di.ui.repository.repositoryexplorer.IUIRole;
import org.pentaho.di.ui.repository.repositoryexplorer.IUIEEUser;
import org.pentaho.di.ui.repository.repositoryexplorer.UIEEObjectRegistery;

public class UIEERepositoryUser extends UIRepositoryUser implements IUIEEUser{
  private IEEUser eeUser;
  public UIEERepositoryUser() {
    super();
    // TODO Auto-generated constructor stub
  }

  public UIEERepositoryUser(IUser user) {
    super(user);
    if(user instanceof IEEUser) {
      eeUser = (IEEUser) user;      
    } 
  }

  public boolean addRole(IUIRole role) {
    return eeUser.addRole(role.getRole());
  }

  public boolean removeRole(IUIRole role) {
    return removeRole(role.getRole().getName());
  }

  public void clearRoles() {
      eeUser.clearRoles();
  }

  public void setRoles(Set<IUIRole> roles) {
    Set<IRole> roleSet = new HashSet<IRole>();
    for(IUIRole role:roles) {
      roleSet.add(role.getRole());
    }
      eeUser.setRoles(roleSet);
  }

  public Set<IUIRole> getRoles() {
    Set<IUIRole> rroles = new HashSet<IUIRole>();
      for(IRole role:eeUser.getRoles()) {
        try {
          rroles.add(UIEEObjectRegistery.getInstance().constructUIRepositoryRole(role));
        } catch(UIObjectCreationException uex) {
          
        }
      }
    return rroles;
  }

  private boolean removeRole(String roleName) {
    IRole roleInfo = null;
      for(IRole role:eeUser.getRoles()) {
        if(role.getName().equals(roleName)) {
          roleInfo = role;
          break;
        }
      }
      if(roleInfo != null) {
        return eeUser.removeRole(roleInfo);
      } else {
        return false;
      }
  }

}
