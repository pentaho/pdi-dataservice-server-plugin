package org.pentaho.di.repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class AbsRoleInfo extends RoleInfo implements ILogicalRole {

  // logical roles bound to a given runtime role
  private List<String> logicalRoles;

  // logical roles bound to a given runtime role
  private List<String> localizedLogicalRoles;

  public AbsRoleInfo() {
    super();
    this.logicalRoles = new ArrayList<String>();    
  }

  public AbsRoleInfo(String name, String description) {
    super(name, description);
    this.logicalRoles = new ArrayList<String>();
  }

  public AbsRoleInfo(String name, String description, Set<UserInfo> users, List<String> logicalRoles) {
    super(name, description, users);
    this.logicalRoles = logicalRoles;
  }

  public void addLogicalRole(String logicalRole) {
    if(!containsLogicalRole(logicalRole)) {
      this.logicalRoles.add(logicalRole);
    }
  }

  public void removeLogicalRole(String logicalRole) {
    if(containsLogicalRole(logicalRole)) {
      this.logicalRoles.remove(logicalRole);
    }
  }

  public List<String> getLogicalRoles() {
    return logicalRoles;
  }

  public void setLogicalRoles(List<String> logicalRoles) {
      this.logicalRoles = logicalRoles;
  }

  public void setLocalizedLogicalRoles(List<String> localizedLogicalRoles) {
    this.localizedLogicalRoles = localizedLogicalRoles;
  }

  public List<String> getLocalizedLogicalRoles() {
    return localizedLogicalRoles;
  }

  public boolean containsLogicalRole(String logicalRole) {
    if(logicalRoles != null) {
      for(String role:logicalRoles) {
        if(role.equals(logicalRole)) {
          return true;
        }
      }
    }
    return false;
  }
}