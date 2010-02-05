package org.pentaho.di.repository.pur;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.repository.RoleInfo;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.platform.engine.security.userroledao.ws.IUserRoleWebService;
import org.pentaho.platform.engine.security.userroledao.ws.ProxyPentahoRole;
import org.pentaho.platform.engine.security.userroledao.ws.ProxyPentahoUser;
import org.pentaho.platform.engine.security.userroledao.ws.UserRoleException;

public class UserRoleHelper {

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

  public static List<RoleInfo> convertToListFromProxyPentahoRoles(ProxyPentahoRole[] roles,
      IUserRoleWebService userRoleWebService, UserRoleLookupCache lookupCache) {
    List<RoleInfo> roleList = new ArrayList<RoleInfo>();
    for (ProxyPentahoRole role : roles) {
      roleList.add(convertFromProxyPentahoRole(userRoleWebService, role, lookupCache));
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

  public static ProxyPentahoRole[] convertToPentahoProxyRoles(Set<RoleInfo> roles) {
    ProxyPentahoRole[] proxyRoles = new ProxyPentahoRole[roles.size()];
    int i = 0;
    for (RoleInfo role : roles) {
      proxyRoles[i++] = convertToPentahoProxyRole(role);
    }
    return proxyRoles;
  }

  public static ProxyPentahoRole[] convertToPentahoProxyRoles(List<RoleInfo> roles) {
    ProxyPentahoRole[] proxyRoles = new ProxyPentahoRole[roles.size()];
    int i = 0;
    for (RoleInfo role : roles) {
      proxyRoles[i++] = convertToPentahoProxyRole(role);
    }
    return proxyRoles;
  }

  public static ProxyPentahoRole convertToPentahoProxyRole(RoleInfo roleInfo) {
    ProxyPentahoRole role = new ProxyPentahoRole();
    role.setName(roleInfo.getName());
    role.setDescription(roleInfo.getDescription());
    return role;
  }

  private static Set<RoleInfo> convertToSetFromProxyPentahoRoles(ProxyPentahoRole[] roles,
      UserRoleLookupCache lookupCache) {
    Set<RoleInfo> roleSet = new HashSet<RoleInfo>();
    for (ProxyPentahoRole role : roles) {
      roleSet.add(lookupCache.lookupRole(role));
    }
    return roleSet;
  }

  public static RoleInfo convertFromProxyPentahoRole(IUserRoleWebService userRoleWebService, ProxyPentahoRole role,
      UserRoleLookupCache lookupCache) {
    RoleInfo roleInfo = new RoleInfo();
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

}
