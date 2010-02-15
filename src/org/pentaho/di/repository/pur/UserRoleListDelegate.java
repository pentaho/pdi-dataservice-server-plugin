package org.pentaho.di.repository.pur;

import java.net.URL;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.ws.BindingProvider;
import javax.xml.ws.Service;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.platform.engine.security.userrole.ws.IUserDetailsRoleListWebService;
import org.pentaho.platform.engine.security.userrole.ws.UserRoleInfo;

public class UserRoleListDelegate {
  IUserDetailsRoleListWebService userDetailsRoleListWebService = null;
  UserRoleInfo userRoleInfo = null;
  
  public UserRoleListDelegate(PurRepositoryMeta repositoryMeta, UserInfo userInfo) {
    try {
      final String url = repositoryMeta.getRepositoryLocation().getUrl() + "/userrolelist?wsdl"; //$NON-NLS-1$
      Service service = Service.create(new URL(url), new QName("http://www.pentaho.org/ws/1.0", //$NON-NLS-1$
          "DefaultUserDetailsRoleListWebServiceService")); //$NON-NLS-1$

      userDetailsRoleListWebService = service.getPort(IUserDetailsRoleListWebService.class);
      // http basic authentication
      ((BindingProvider) userDetailsRoleListWebService).getRequestContext()
          .put(BindingProvider.USERNAME_PROPERTY, userInfo.getLogin());
      ((BindingProvider) userDetailsRoleListWebService).getRequestContext().put(BindingProvider.PASSWORD_PROPERTY,
          userInfo.getPassword());
      userRoleInfo = userDetailsRoleListWebService.getUserRoleInfo();
    } catch (Exception e) {

    }

  }

  public List<String> getAllRoles() throws KettleException {
    return userRoleInfo.getRoles();
  }
  
  public List<String> getAllUsers() throws KettleException {
    return userRoleInfo.getUsers();
  }
}
