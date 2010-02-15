package org.pentaho.di.repository;

import java.util.List;
import java.util.Map;

import org.pentaho.di.core.exception.KettleException;

public interface IAbsAdmin extends IAbsCore{

  public void setLogicalRoles(String rolename , List<String> logicalRoles)  throws KettleException;
  public List<String> getLogicalRoles(String rolename, String locale)  throws KettleException;
  public Map<String, List<String>> getAllLogicalRoles(String locale)  throws KettleException;
  public List<String> getLogicalRolesToDisplay(String locale)  throws KettleException;
}
