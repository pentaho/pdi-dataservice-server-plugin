package org.pentaho.di.repository.pur;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.ws.BindingProvider;
import javax.xml.ws.Service;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.RoleInfo;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.platform.engine.security.userroledao.ws.IUserRoleWebService;
import org.pentaho.platform.engine.security.userroledao.ws.ProxyPentahoRole;
import org.pentaho.platform.engine.security.userroledao.ws.ProxyPentahoUser;

public class UserRoleDelegate {

  IUserRoleWebService userRoleWebService = null;

  UserRoleLookupCache lookupCache = null;

  public UserRoleDelegate(PurRepositoryMeta repositoryMeta, UserInfo userInfo) {
    try {
      final String url = repositoryMeta.getRepositoryLocation().getUrl() + "/userroleadmin?wsdl"; //$NON-NLS-1$
      Service service = Service.create(new URL(url), new QName("http://www.pentaho.org/ws/1.0", //$NON-NLS-1$
          "UserRoleWebServiceService"));//$NON-NLS-1$
      userRoleWebService = service.getPort(IUserRoleWebService.class);
      ((BindingProvider) userRoleWebService).getRequestContext().put(BindingProvider.USERNAME_PROPERTY,
          userInfo.getLogin());
      ((BindingProvider) userRoleWebService).getRequestContext().put(BindingProvider.PASSWORD_PROPERTY,
          userInfo.getPassword());
      lookupCache = new UserRoleLookupCache(userRoleWebService);
    } catch (Exception e) {

    }
  }

  public void createUser(UserInfo newUser) throws KettleException {
    try {
      ProxyPentahoUser user = UserRoleHelper.convertToPentahoProxyUser(newUser);
      userRoleWebService.createUser(user);
      userRoleWebService.setRoles(user, UserRoleHelper.convertToPentahoProxyRoles(newUser.getRoles()));
    } catch (Exception e) {
      throw new KettleException(e);
    }
  }

  public void deleteUsers(List<UserInfo> users) throws KettleException {
    try {
      userRoleWebService.deleteUsers(UserRoleHelper.convertToPentahoProxyUsers(users));
      lookupCache.removeUsersFromLookupSet(users);
    } catch (Exception e) {
      throw new KettleException(e);
    }
  }

  public void deleteUser(String name) throws KettleException {
    try {
      ProxyPentahoUser user = userRoleWebService.getUser(name);
      if (user != null) {
        ProxyPentahoUser[] users = new ProxyPentahoUser[1];
        users[0] = user;
        userRoleWebService.deleteUsers(users);
      } else {
        throw new KettleException("Unable to delete user with name : " + name);
      }
    } catch (Exception e) {
      throw new KettleException(e);
    }
  }

  public void setUsers(List<UserInfo> users) throws KettleException {
    // TODO Figure out what to do here
  }

  public UserInfo getUser(String name, String password) throws KettleException {
    UserInfo userInfo = null;
    try {
      ProxyPentahoUser user = userRoleWebService.getUser(name);
      if (user != null && user.getName().equals(name) && user.getPassword().equals(password)) {
        userInfo = UserRoleHelper.convertFromProxyPentahoUser(userRoleWebService, user, lookupCache);
      }
    } catch (Exception e) {
      throw new KettleException(e);
    }
    return userInfo;
  }

  public UserInfo getUser(String name) throws KettleException {
    UserInfo userInfo = null;
    try {
      ProxyPentahoUser user = userRoleWebService.getUser(name);
      if (user != null && user.getName().equals(name)) {
        userInfo = UserRoleHelper.convertFromProxyPentahoUser(userRoleWebService, user, lookupCache);
      }
    } catch (Exception e) {
      throw new KettleException(e);
    }
    return userInfo;
  }

  public List<UserInfo> getUsers() throws KettleException {
    try {
      return UserRoleHelper.convertToListFromProxyPentahoUsers(userRoleWebService.getUsers(), userRoleWebService,
          lookupCache);
    } catch (Exception e) {
      throw new KettleException(e);
    }
  }

  public void updateUser(UserInfo user) throws KettleException {
    try {
      ProxyPentahoUser proxyUser = UserRoleHelper.convertToPentahoProxyUser(user);
      userRoleWebService.updateUser(proxyUser);
      userRoleWebService.setRoles(proxyUser, UserRoleHelper.convertToPentahoProxyRoles(user.getRoles()));
      lookupCache.updateUserInLookupSet(user);
    } catch (Exception e) {
      throw new KettleException(e);
    }
  }

  public void createRole(RoleInfo newRole) throws KettleException {
    try {
      ProxyPentahoRole role = UserRoleHelper.convertToPentahoProxyRole(newRole);
      userRoleWebService.createRole(role);
      userRoleWebService.setUsers(role, UserRoleHelper.convertToPentahoProxyUsers(newRole.getUsers()));
    } catch (Exception ure) {
      throw new KettleException(ure);
    }
  }

  public void deleteRoles(List<RoleInfo> roles) throws KettleException {
    try {
      userRoleWebService.deleteRoles(UserRoleHelper.convertToPentahoProxyRoles(roles));
      lookupCache.removeRolesFromLookupSet(roles);
    } catch (Exception ure) {
      throw new KettleException(ure);
    }

  }

  public RoleInfo getRole(String name) throws KettleException {
    try {
      return UserRoleHelper.convertFromProxyPentahoRole(userRoleWebService, UserRoleHelper.getProxyPentahoRole(
          userRoleWebService, name), lookupCache);
    } catch (Exception e) {
      throw new KettleException(e);
    }
  }

  public List<RoleInfo> getRoles() throws KettleException {
    try {
      return UserRoleHelper.convertToListFromProxyPentahoRoles(userRoleWebService.getRoles(), userRoleWebService,
          lookupCache);
    } catch (Exception ure) {
      throw new KettleException(ure);
    }
  }

  public void updateRole(RoleInfo role) throws KettleException {
    try {
      List<String> users = new ArrayList<String>();
      for (UserInfo user : role.getUsers()) {
        users.add(user.getLogin());
      }
      userRoleWebService.updateRole(role.getName(), role.getDescription(), users);
      lookupCache.updateRoleInLookupSet(role);
    } catch (Exception ure) {
      throw new KettleException(ure);
    }

  }

  public void deleteRole(String name) throws KettleException {
    try {
      ProxyPentahoRole roleToDelete = UserRoleHelper.getProxyPentahoRole(userRoleWebService, name);
      if (roleToDelete != null) {
        ProxyPentahoRole[] roleArray = new ProxyPentahoRole[1];
        roleArray[0] = roleToDelete;
        userRoleWebService.deleteRoles(roleArray);
      } else {
        throw new KettleException("Unable to delete role with name : " + name);
      }
    } catch (Exception ure) {
      throw new KettleException(ure);
    }
  }

  public void setRoles(List<RoleInfo> roles) throws KettleException {
    // TODO Figure out what to do here
  }
}
