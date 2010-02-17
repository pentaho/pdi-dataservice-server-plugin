package org.pentaho.di.repository.pur;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.IRole;
import org.pentaho.di.repository.RepositorySecurityManager;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.platform.engine.security.userroledao.ws.IUserRoleWebService;
import org.pentaho.platform.engine.security.userroledao.ws.ProxyPentahoRole;
import org.pentaho.platform.engine.security.userroledao.ws.ProxyPentahoUser;
import org.pentaho.platform.engine.security.userroledao.ws.UserRoleException;
import org.pentaho.platform.engine.security.userroledao.ws.UserRoleSecurityInfo;
import org.pentaho.platform.engine.security.userroledao.ws.UserToRoleAssignment;

public class UserRoleHelper {

  public static List<UserInfo> convertFromProxyPentahoUsers(UserRoleSecurityInfo info, RepositorySecurityManager rsm) {
    List<UserInfo> userList = new ArrayList<UserInfo>();
    List<ProxyPentahoUser> users = info.getUsers();
    List<UserToRoleAssignment>  assignments = info.getAssignments();
    for(ProxyPentahoUser user:users) {
      userList.add(convertFromProxyPentahoUser(user, assignments, rsm));
    }
    return userList;
  }
  public static List<IRole> convertToListFromProxyPentahoRoles(UserRoleSecurityInfo info, RepositorySecurityManager rsm) {
    List<IRole> roleList = new ArrayList<IRole>();
    List<ProxyPentahoRole> roles = info.getRoles();
    List<UserToRoleAssignment>  assignments = info.getAssignments();
    for (ProxyPentahoRole role : roles) {
      roleList.add(convertFromProxyPentahoRole(role, assignments, rsm));
    }
    return roleList;
  }
  

  public static ProxyPentahoRole getProxyPentahoRole(IUserRoleWebService userRoleWebService, String name)
      throws UserRoleException {
    ProxyPentahoRole roleToFind = null;
    ProxyPentahoRole[] roles = userRoleWebService.getRoles();
    if (roles != null && roles.length > 0) {
      for (ProxyPentahoRole role : roles) {
        if (role.getName().equals(name)) {
          roleToFind = role;
          break;
        }
      }
    }
    return roleToFind;
  }

  public static List<UserInfo> convertToListFromProxyPentahoUsers(ProxyPentahoUser[] users,
      IUserRoleWebService userRoleWebService, UserRoleLookupCache lookupCache) {
    List<UserInfo> userList = new ArrayList<UserInfo>();
    for (ProxyPentahoUser user : users) {
      userList.add(convertFromProxyPentahoUser(userRoleWebService, user, lookupCache));
    }
    return userList;
  }

  public static List<IRole> convertToListFromProxyPentahoRoles(ProxyPentahoRole[] roles,
      IUserRoleWebService userRoleWebService, UserRoleLookupCache lookupCache, RepositorySecurityManager rsm) {
    List<IRole> roleList = new ArrayList<IRole>();
    for (ProxyPentahoRole role : roles) {
      roleList.add(convertFromProxyPentahoRole(userRoleWebService, role, lookupCache, rsm));
    }
    return roleList;
  }

  public static ProxyPentahoUser[] convertToPentahoProxyUsers(Set<UserInfo> users) {
    ProxyPentahoUser[] proxyUsers = new ProxyPentahoUser[users.size()];
    int i = 0;
    for (UserInfo user : users) {
      proxyUsers[i++] = convertToPentahoProxyUser(user);
    }
    return proxyUsers;
  }

  public static ProxyPentahoUser[] convertToPentahoProxyUsers(List<UserInfo> users) {
    ProxyPentahoUser[] proxyUsers = new ProxyPentahoUser[users.size()];
    int i = 0;
    for (UserInfo user : users) {
      proxyUsers[i++] = convertToPentahoProxyUser(user);
    }
    return proxyUsers;
  }

  public static ProxyPentahoUser convertToPentahoProxyUser(UserInfo userInfo) {
    ProxyPentahoUser user = new ProxyPentahoUser();
    user.setName(userInfo.getLogin());
    // Since we send the empty password to the client, if the client has not modified the password then we do change it
    if(!StringUtils.isEmpty(userInfo.getPassword())) {
      user.setPassword(userInfo.getPassword());      
    }
    user.setDescription(userInfo.getDescription());
    return user;
  }

  public static ProxyPentahoRole[] convertToPentahoProxyRoles(Set<IRole> roles) {
    ProxyPentahoRole[] proxyRoles = new ProxyPentahoRole[roles.size()];
    int i = 0;
    for (IRole role : roles) {
      proxyRoles[i++] = convertToPentahoProxyRole(role);
    }
    return proxyRoles;
  }

  public static ProxyPentahoRole[] convertToPentahoProxyRoles(List<IRole> roles) {
    ProxyPentahoRole[] proxyRoles = new ProxyPentahoRole[roles.size()];
    int i = 0;
    for (IRole role : roles) {
      proxyRoles[i++] = convertToPentahoProxyRole(role);
    }
    return proxyRoles;
  }

  public static ProxyPentahoRole convertToPentahoProxyRole(IRole roleInfo) {
    ProxyPentahoRole role = new ProxyPentahoRole();
    role.setName(roleInfo.getName());
    role.setDescription(roleInfo.getDescription());
    return role;
  }

  private static Set<IRole> convertToSetFromProxyPentahoRoles(ProxyPentahoRole[] roles,
      UserRoleLookupCache lookupCache) {
    Set<IRole> roleSet = new HashSet<IRole>();
    for (ProxyPentahoRole role : roles) {
      roleSet.add(lookupCache.lookupRole(role));
    }
    return roleSet;
  }

  public static IRole convertFromProxyPentahoRole(IUserRoleWebService userRoleWebService, ProxyPentahoRole role,
      UserRoleLookupCache lookupCache, RepositorySecurityManager rsm) {
    IRole roleInfo = null;
    try {
      roleInfo = rsm.constructRole();
    } catch (KettleException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    roleInfo.setDescription(role.getDescription());
    roleInfo.setName(role.getName());
    try {
      roleInfo.setUsers(convertToSetFromProxyPentahoUsers(userRoleWebService.getUsersForRole(role), lookupCache));
    } catch (UserRoleException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return roleInfo;
  }

  public static UserInfo convertFromProxyPentahoUser(IUserRoleWebService userRoleWebService, ProxyPentahoUser user,
      UserRoleLookupCache lookupCache) {
    UserInfo userInfo = new UserInfo();
    userInfo.setDescription(user.getDescription());
    userInfo.setPassword(user.getPassword());
    userInfo.setLogin(user.getName());
    userInfo.setName(user.getName());
    try {
      userInfo.setRoles(convertToSetFromProxyPentahoRoles(userRoleWebService.getRolesForUser(user), lookupCache));
    } catch (UserRoleException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return userInfo;
  }

  private static Set<UserInfo> convertToSetFromProxyPentahoUsers(ProxyPentahoUser[] users,
      UserRoleLookupCache lookupCache) {
    Set<UserInfo> userSet = new HashSet<UserInfo>();
    for (ProxyPentahoUser user : users) {
      userSet.add(lookupCache.lookupUser(user));
    }
    return userSet;
  }
  private static Set<IRole> convertToSetFromProxyPentahoRoles(ProxyPentahoRole[] roles, RepositorySecurityManager rsm) {
    Set<IRole> roleSet = new HashSet<IRole>();
    for (ProxyPentahoRole role : roles) {
      IRole roleInfo = null;
      try {
        roleInfo = rsm.constructRole();
      } catch (KettleException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      roleInfo.setDescription(role.getDescription());
      roleInfo.setName(role.getName());
      roleSet.add(roleInfo);
    }
    return roleSet;
  }
  public static UserInfo convertToUserInfo(ProxyPentahoUser user, ProxyPentahoRole[] roles, RepositorySecurityManager rsm) {
    UserInfo userInfo = new UserInfo();
    userInfo.setDescription(user.getDescription());
    userInfo.setPassword(user.getPassword());
    userInfo.setLogin(user.getName());
    userInfo.setName(user.getName());
    userInfo.setRoles(convertToSetFromProxyPentahoRoles(roles, rsm));
    return userInfo;
  }

  public static IRole convertFromProxyPentahoRole(ProxyPentahoRole role, List<UserToRoleAssignment> assignments, RepositorySecurityManager rsm) {
    IRole roleInfo = null;
    try {
      roleInfo = rsm.constructRole();
    } catch (KettleException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    roleInfo.setDescription(role.getDescription());
    roleInfo.setName(role.getName());
    roleInfo.setUsers(getUsersForRole(role.getName(), assignments));
    return roleInfo;
  }
  public static UserInfo convertFromProxyPentahoUser(ProxyPentahoUser user, List<UserToRoleAssignment> assignments, RepositorySecurityManager rsm) {
    UserInfo userInfo = new UserInfo();
    userInfo.setDescription(user.getDescription());
    userInfo.setPassword(user.getPassword());
    userInfo.setLogin(user.getName());
    userInfo.setName(user.getName());
    userInfo.setRoles(getRolesForUser(user.getName(), assignments, rsm));
    return userInfo;
  }
  
  public static Set<UserInfo> getUsersForRole(String name, List<UserToRoleAssignment> assignments) {
    Set<UserInfo> users = new HashSet<UserInfo>();
    for(UserToRoleAssignment assignment:assignments) {
      if(name.equals(assignment.getRoleId())) {
        users.add(new UserInfo(assignment.getUserId()));
      }
    }
    return users;
  }  
  
  public static Set<IRole> getRolesForUser(String name, List<UserToRoleAssignment> assignments, RepositorySecurityManager rsm) {
    Set<IRole> roles = new HashSet<IRole>();
    for(UserToRoleAssignment assignment:assignments) {
      if(name.equals(assignment.getUserId())) {
        IRole role = null;
        try {
          role = rsm.constructRole();
        } catch (KettleException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        role.setName(assignment.getRoleId());
        roles.add(role);
      }
    }
    return roles;
  }
}
