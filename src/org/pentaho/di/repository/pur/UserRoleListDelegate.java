package org.pentaho.di.repository.pur;

import java.net.URL;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.ws.BindingProvider;
import javax.xml.ws.Service;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.platform.api.engine.IUserDetailsRoleListService;
import org.pentaho.platform.engine.security.userrole.ws.IUserDetailsRoleListWebService;
import org.pentaho.platform.engine.security.userrole.ws.UserDetailsRoleListServiceToWebServiceAdapter;

public class UserRoleListDelegate {
  IUserDetailsRoleListService userDetailsRoleListService;
  
  public UserRoleListDelegate(PurRepositoryMeta repositoryMeta, UserInfo userInfo) {
    try {
      final String url = repositoryMeta.getRepositoryLocation().getUrl() + "/userDetailsRoleListService?wsdl";
      Service service = Service.create(new URL(url), new QName("http://www.pentaho.org/ws/1.0",
          "DefaultUserDetailsRoleListWebServiceService"));

      IUserDetailsRoleListWebService userDetailsRoleListWebService = service.getPort(IUserDetailsRoleListWebService.class);
      // http basic authentication
      ((BindingProvider) userDetailsRoleListWebService).getRequestContext()
          .put(BindingProvider.USERNAME_PROPERTY, userInfo.getLogin());
      ((BindingProvider) userDetailsRoleListWebService).getRequestContext().put(BindingProvider.PASSWORD_PROPERTY,
          userInfo.getPassword());

      userDetailsRoleListService = new UserDetailsRoleListServiceToWebServiceAdapter(userDetailsRoleListWebService);
    } catch (Exception e) {

    }

  }

  @SuppressWarnings("unchecked")
  public List<String> getAllRoles() throws KettleException {
    return userDetailsRoleListService.getAllRoles();
  }
  
  @SuppressWarnings("unchecked")
  public List<String> getAllUsers() throws KettleException {
    return userDetailsRoleListService.getAllUsers();
  }
  
  @SuppressWarnings("unchecked")
  public List<String> getAllUsersInRole(String role) throws KettleException {
    return userDetailsRoleListService.getAllUsersInRole(role);
  }
  
  @SuppressWarnings("unchecked")
  public List<String> getRolesForUser(String userName) throws KettleException {
    return userDetailsRoleListService.getRolesForUser(userName);
  }

}
