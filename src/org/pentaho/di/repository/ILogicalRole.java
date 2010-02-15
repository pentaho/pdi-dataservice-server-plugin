package org.pentaho.di.repository;

import java.util.List;

public interface ILogicalRole {

  public void addLogicalRole(String logicalRole);
  public void removeLogicalRole(String logicalRole);
  public void setLogicalRoles(List<String> logicalRoles);
  public List<String> getLogicalRoles();
}
