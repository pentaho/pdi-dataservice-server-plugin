package org.pentaho.di.repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.pur.PurRepository;
import org.pentaho.di.repository.pur.PurRepositoryMeta;

public class AbsSecurityAdmin extends AbsSecurityCore implements IAbsAdmin{
  List<String> logicalRoles = new ArrayList<String>();

  public AbsSecurityAdmin(Repository repository, RepositoryMeta repositoryMeta, UserInfo userInfo) {
    super((PurRepository) repository, (PurRepositoryMeta) repositoryMeta, userInfo);
    logicalRoles.add("Create Content");
    logicalRoles.add("Read Content");
    logicalRoles.add("Administer Security");
  }

  public Map<String, List<String>> getAllLogicalRoles(String locale) throws KettleException {
    if(this.isAllowed("Administer Security")) {
      Map<String, List<String>> map = new HashMap<String, List<String>>();
      map.put("joe", logicalRoles);
      map.put("suzy", logicalRoles);
      map.put("tiffany", logicalRoles);
      map.put("pat", logicalRoles);
      return map;
    } else {
      return null;
    } 
  }

  public List<String> getLogicalRoles(String rolename, String locale) throws KettleException {
    if(this.isAllowed("Administer Security")) {
      return logicalRoles;
    }
    return null;
  }

  public void setLogicalRoles(String rolename, List<String> logicalRoles) throws KettleException {
    // TODO If the user has ADMINISTER SECURITY role then perform this operation
    // delegate.setLogicalRoles(rolename, permissions);
  }

  public List<String> getLogicalRolesToDisplay(String locale) throws KettleException {
    if(this.isAllowed("Administer Security")) {
      return logicalRoles;
    }
    return null;
  }
}
