package org.pentaho.di.repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class AbsRoleInfo extends RoleInfo implements ILogicalRole {
  
  private List<String> logicalRoles;
  
  public AbsRoleInfo() {
    super();
    this.logicalRoles = new ArrayList<String>();    
  }
  
  public AbsRoleInfo(String name, String description, Set<UserInfo> users, List<String> logicalRoles) {
    super(name, description, users);
    this.logicalRoles = logicalRoles;
  }

  public void addLogicalRole(String logicalRole) {
    this.logicalRoles.add(logicalRole);
  }

  public void removeLogicalRole(String logicalRole) {
    this.logicalRoles.remove(logicalRole);
  }

  public List<String> getLogicalRoles() {
    return logicalRoles;
  }

  public void setLogicalRoles(List<String> logicalRoles) {
      this.logicalRoles = logicalRoles;
  }
}