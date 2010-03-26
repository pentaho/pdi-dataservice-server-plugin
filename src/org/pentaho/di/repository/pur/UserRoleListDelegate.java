package org.pentaho.di.repository.pur;

import java.util.List;

import org.apache.commons.logging.Log;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.IUser;
import org.pentaho.di.repository.WsFactory;
import org.pentaho.platform.engine.security.userrole.ws.IUserDetailsRoleListWebService;
import org.pentaho.platform.engine.security.userrole.ws.UserRoleInfo;

public class UserRoleListDelegate {
  IUserDetailsRoleListWebService userDetailsRoleListWebService;
  UserRoleInfo userRoleInfo;
  Log logger;
  public UserRoleListDelegate() {
    
  }
  public UserRoleListDelegate(PurRepositoryMeta repositoryMeta, IUser userInfo, Log logger) {
    try {
      this.logger = logger;
      userDetailsRoleListWebService = WsFactory.createService(repositoryMeta, "userRoleListService", userInfo //$NON-NLS-1$
          .getLogin(), userInfo.getPassword(), IUserDetailsRoleListWebService.class);
      updateUserRoleList();
    } catch (Exception e) {
      this.logger.error(BaseMessages.getString("UserRoleListDelegate.ERROR_0001_UNABLE_TO_INITIALIZE_USER_ROLE_LIST_WEBSVC"), e); //$NON-NLS-1$
    }

  }

  public List<String> getAllRoles() throws KettleException {
    return userRoleInfo.getRoles();
  }
  
  public List<String> getAllUsers() throws KettleException {
    return userRoleInfo.getUsers();
  }
  public void updateUserRoleList() {
    userRoleInfo = userDetailsRoleListWebService.getUserRoleInfo();
  }
  public IUserDetailsRoleListWebService getUserDetailsRoleListWebService() {
    return userDetailsRoleListWebService;
  }
  public void setUserDetailsRoleListWebService(IUserDetailsRoleListWebService userDetailsRoleListWebService) {
    this.userDetailsRoleListWebService = userDetailsRoleListWebService;
  }
  public UserRoleInfo getUserRoleInfo() {
    return userRoleInfo;
  }
  public void setUserRoleInfo(UserRoleInfo userRoleInfo) {
    this.userRoleInfo = userRoleInfo;
  }

}
