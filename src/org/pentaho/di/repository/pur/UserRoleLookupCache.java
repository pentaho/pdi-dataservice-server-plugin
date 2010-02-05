package org.pentaho.di.repository.pur;

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

public class UserRoleLookupCache {
  Set<UserInfo> userInfoSet;

  Set<RoleInfo> roleInfoSet;

  public UserRoleLookupCache(IUserRoleWebService userRoleWebService) {
    try {
      userInfoSet = new HashSet<UserInfo>();
      for (ProxyPentahoUser user : userRoleWebService.getUsers()) {
        userInfoSet.add(createUserInfo(user));
      }
      roleInfoSet = new HashSet<RoleInfo>();
      for (ProxyPentahoRole role : userRoleWebService.getRoles()) {
        roleInfoSet.add(createRoleInfo(role));
      }
    } catch (UserRoleException ure) {

    }
  }

  public UserInfo lookupUser(ProxyPentahoUser proxyUser) {
    for (UserInfo user : userInfoSet) {
      if (user.getLogin().equals(proxyUser.getName())) {
        return user;
      }
    }
    return addUserToLookupSet(proxyUser);
  }

  public RoleInfo lookupRole(ProxyPentahoRole proxyRole) {
    for (RoleInfo role : roleInfoSet) {
      if (role.getName().equals(proxyRole.getName())) {
        return role;
      }
    }
    return addRoleToLookupSet(proxyRole);
  }

  public void updateUserInLookupSet(UserInfo user) {
    UserInfo userInfoToUpdate = null;
    for (UserInfo userInfo : userInfoSet) {
      if (userInfo.getLogin().equals(user.getLogin())) {
        userInfoToUpdate = userInfo;
        break;
      }
    }
    userInfoToUpdate.setDescription(user.getDescription());
    if(!StringUtils.isEmpty(user.getPassword())) {
      userInfoToUpdate.setPassword(user.getPassword());        
    }
  }

  public void updateRoleInLookupSet(RoleInfo role) {
    RoleInfo roleInfoToUpdate = null;
    for (RoleInfo roleInfo : roleInfoSet) {
      if (roleInfo.getName().equals(role.getName())) {
        roleInfoToUpdate = roleInfo;
        break;
      }
    }
    roleInfoToUpdate.setDescription(role.getDescription());
    roleInfoSet.remove(roleInfoToUpdate);
  }

  public void removeUsersFromLookupSet(List<UserInfo> users) {
    for (UserInfo user : users) {
      removeUserFromLookupSet(user);
    }
  }

  public void removeRolesFromLookupSet(List<RoleInfo> roles) {
    for (RoleInfo role : roles) {
      removeRoleFromLookupSet(role);
    }
  }

  private void removeUserFromLookupSet(UserInfo user) {
    UserInfo userToRemove = null;
    for (UserInfo userInfo : userInfoSet) {
      if (userInfo.getLogin().equals(user.getLogin())) {
        userToRemove = userInfo;
        break;
      }
      userInfoSet.remove(userToRemove);
    }
  }

  private void removeRoleFromLookupSet(RoleInfo role) {
    RoleInfo roleToRemove = null;
    for (RoleInfo roleInfo : roleInfoSet) {
      if (roleInfo.getName().equals(role.getName())) {
        roleToRemove = roleInfo;
        break;
      }
      roleInfoSet.remove(roleToRemove);
    }
  }

  private UserInfo addUserToLookupSet(ProxyPentahoUser user) {
    UserInfo userInfo = createUserInfo(user);
    userInfoSet.add(userInfo);
    return userInfo;
  }

  private RoleInfo addRoleToLookupSet(ProxyPentahoRole role) {
    RoleInfo roleInfo = createRoleInfo(role);
    roleInfoSet.add(roleInfo);
    return roleInfo;
  }

  private UserInfo createUserInfo(ProxyPentahoUser user) {
    UserInfo userInfo = new UserInfo();
    userInfo.setDescription(user.getDescription());
    userInfo.setLogin(user.getName());
    userInfo.setName(user.getName());
    userInfo.setPassword(user.getPassword());
    return userInfo;
  }

  private RoleInfo createRoleInfo(ProxyPentahoRole role) {
    RoleInfo roleInfo = new RoleInfo();
    roleInfo.setDescription(role.getDescription());
    roleInfo.setName(role.getName());
    return roleInfo;
  }

}
