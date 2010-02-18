package org.pentaho.di.ui.repository.repositoryexplorer.abs.model;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.AbsRoleInfo;
import org.pentaho.di.repository.IRole;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.ProfileMeta;
import org.pentaho.di.repository.RepositorySecurityManager;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.di.repository.ProfileMeta.Permission;
import org.pentaho.di.ui.repository.repositoryexplorer.model.IUIRole;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryRole;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryUser;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UISecurity;

public class RepsitoryUserTestImpl implements RepositorySecurityManager{
  private List<UserInfo> users;
  private List<IRole> roles;
  private UISecurity security;
  public RepsitoryUserTestImpl() {
    users = new ArrayList<UserInfo>();
    roles = new ArrayList<IRole>();
    security = new UISecurity();
    List<IUIRole> rroles;
    List<UIRepositoryUser> rusers;
    UserInfo joeUser;
    UserInfo patUser;  
    UserInfo suzyUser;
    UserInfo tiffanyUser;
    IRole adminRole = new AbsRoleInfo("Admin","Super User");
    IRole anonymousRole = new AbsRoleInfo("Anonymous","User has not logged in");
    IRole authenticatedRole =  new AbsRoleInfo("Authenticated","User has logged in");
    IRole ceoRole =  new AbsRoleInfo("ceo","Chief Executive Officer");
    IRole ctoRole =  new AbsRoleInfo("cto","Chief Technology Officer");
    IRole devRole =  new AbsRoleInfo("dev","Developer");
    IRole devmgrRole =  new AbsRoleInfo("devmgr","Development Manager");
    IRole isRole =  new AbsRoleInfo("is","Information Services");
    roles.add(adminRole);
    roles.add(anonymousRole);
    roles.add(authenticatedRole);
    roles.add(ceoRole);
    roles.add(ctoRole);
    roles.add(devRole);
    roles.add(devmgrRole);
    roles.add(isRole);
    
    joeUser = new UserInfo("joe", "password", "joe","joe", true);
    patUser = new UserInfo("pat", "password", "pat","pat", true);
    suzyUser = new UserInfo("suzy", "password", "suzy","suzy", true);
    tiffanyUser = new UserInfo("tiffany", "password", "tiffany","tiffany", true);
    
    joeUser.addRole(roles.get(0));
    joeUser.addRole(roles.get(2));
    joeUser.addRole(roles.get(3));

    suzyUser.addRole(roles.get(2));
    suzyUser.addRole(roles.get(4));
    suzyUser.addRole(roles.get(7));

    patUser.addRole(roles.get(2));
    patUser.addRole(roles.get(5));

    tiffanyUser.addRole(roles.get(2));
    tiffanyUser.addRole(roles.get(5));
    tiffanyUser.addRole(roles.get(6));
    
    adminRole.addUser(joeUser);
    adminRole.addUser(patUser);
    
    anonymousRole.addUser(tiffanyUser);
    
    authenticatedRole.addUser(joeUser);
    authenticatedRole.addUser(patUser);
    authenticatedRole.addUser(suzyUser);
    authenticatedRole.addUser(tiffanyUser);
    
    ceoRole.addUser(joeUser);
    
    ctoRole.addUser(patUser);
    
    devmgrRole.addUser(joeUser);
    devmgrRole.addUser(patUser);
    
    isRole.addUser(joeUser);
    isRole.addUser(suzyUser);
    
    users.add(joeUser);
    users.add(patUser);
    users.add(suzyUser);
    users.add(tiffanyUser);
    rroles = new ArrayList<IUIRole>();
    for(IRole roleInfo:roles) {
      IUIRole role = new UIRepositoryRole(roleInfo);
      rroles.add(role);
    }
    rusers = new ArrayList<UIRepositoryUser>();
    for(UserInfo userInfo:users) {
      rusers.add(new UIRepositoryUser(userInfo));
    }      
    security.setUserList(rusers);
    security.setRoleList(rroles);
  }
  public IRole constructRole() throws KettleException {
    // TODO Auto-generated method stub
    return null;
  }

  public void createRole(IRole arg0) throws KettleException {
    // TODO Auto-generated method stub
    
  }

  public void delProfile(ObjectId arg0) throws KettleException {
    // TODO Auto-generated method stub
    
  }

  public void delUser(ObjectId arg0) throws KettleException {
    // TODO Auto-generated method stub
    
  }

  public void delUser(String arg0) throws KettleException {
    // TODO Auto-generated method stub
    
  }

  public void deleteRole(String arg0) throws KettleException {
    // TODO Auto-generated method stub
    
  }

  public void deleteRoles(List<IRole> arg0) throws KettleException {
    // TODO Auto-generated method stub
    
  }

  public void deleteUsers(List<UserInfo> arg0) throws KettleException {
    // TODO Auto-generated method stub
    
  }

  public List<String> getAllRoles() throws KettleException {
    // TODO Auto-generated method stub
    return null;
  }

  public List<String> getAllUsers() throws KettleException {
    // TODO Auto-generated method stub
    return null;
  }

  public ObjectId[] getPermissionIDs(ObjectId arg0) throws KettleException {
    // TODO Auto-generated method stub
    return null;
  }

  public ObjectId getProfileID(String arg0) throws KettleException {
    // TODO Auto-generated method stub
    return null;
  }

  public String[] getProfiles() throws KettleException {
    // TODO Auto-generated method stub
    return null;
  }

  public IRole getRole(String arg0) throws KettleException {
    // TODO Auto-generated method stub
    return null;
  }

  public List<IRole> getRoles() throws KettleException {
    // TODO Auto-generated method stub
    return roles;
  }

  public ObjectId getUserID(String arg0) throws KettleException {
    // TODO Auto-generated method stub
    return null;
  }

  public ObjectId[] getUserIDs() throws KettleException {
    // TODO Auto-generated method stub
    return null;
  }

  public UserInfo getUserInfo() {
    // TODO Auto-generated method stub
    return null;
  }

  public String[] getUserLogins() throws KettleException {
    // TODO Auto-generated method stub
    return null;
  }

  public List<UserInfo> getUsers() throws KettleException {
    // TODO Auto-generated method stub
    return users;
  }

  public Permission loadPermission(ObjectId arg0) throws KettleException {
    // TODO Auto-generated method stub
    return null;
  }

  public ProfileMeta loadProfileMeta(ObjectId arg0) throws KettleException {
    // TODO Auto-generated method stub
    return null;
  }

  public UserInfo loadUserInfo(String arg0) throws KettleException {
    // TODO Auto-generated method stub
    return null;
  }

  public UserInfo loadUserInfo(String arg0, String arg1) throws KettleException {
    // TODO Auto-generated method stub
    return null;
  }

  public void renameProfile(ObjectId arg0, String arg1) throws KettleException {
    // TODO Auto-generated method stub
    
  }

  public void renameUser(ObjectId arg0, String arg1) throws KettleException {
    // TODO Auto-generated method stub
    
  }

  public void saveProfile(ProfileMeta arg0) throws KettleException {
    // TODO Auto-generated method stub
    
  }

  public void saveUserInfo(UserInfo arg0) throws KettleException {
    // TODO Auto-generated method stub
    
  }

  public void setRoles(List<IRole> arg0) throws KettleException {
    // TODO Auto-generated method stub
    
  }

  public void setUserInfo(UserInfo arg0) {
    // TODO Auto-generated method stub
    
  }

  public void setUsers(List<UserInfo> arg0) throws KettleException {
    // TODO Auto-generated method stub
    
  }

  public void updateRole(IRole arg0) throws KettleException {
    // TODO Auto-generated method stub
    
  }

  public void updateUser(UserInfo arg0) throws KettleException {
    // TODO Auto-generated method stub
    
  }

}
