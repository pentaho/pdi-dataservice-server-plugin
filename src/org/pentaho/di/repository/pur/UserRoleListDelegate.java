package org.pentaho.di.repository.pur;

import java.net.URL;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.ws.BindingProvider;
import javax.xml.ws.Service;

import org.apache.commons.logging.Log;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.IUser;
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
      final String url = repositoryMeta.getRepositoryLocation().getUrl() + "/userRoleListService?wsdl"; //$NON-NLS-1$
      Service service = Service.create(new URL(url), new QName("http://www.pentaho.org/ws/1.0", //$NON-NLS-1$
          "userRoleListService")); //$NON-NLS-1$

      userDetailsRoleListWebService = service.getPort(IUserDetailsRoleListWebService.class);
      // http basic authentication
      ((BindingProvider) userDetailsRoleListWebService).getRequestContext()
          .put(BindingProvider.USERNAME_PROPERTY, userInfo.getLogin());
      ((BindingProvider) userDetailsRoleListWebService).getRequestContext().put(BindingProvider.PASSWORD_PROPERTY,
          userInfo.getPassword());
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
