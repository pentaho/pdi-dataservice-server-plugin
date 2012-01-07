/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.repository.pur;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.platform.engine.security.userrole.ws.IUserDetailsRoleListWebService;
import org.pentaho.platform.engine.security.userrole.ws.UserRoleInfo;

public class UserRoleListDelegateTest implements java.io.Serializable {
  static final long serialVersionUID = -125535373810768433L; /* EESOURCE: UPDATE SERIALVERUID */

  UserRoleListDelegate listDelegate;
  IUserDetailsRoleListWebService service;
  static List<String> roles;
  static List<String> users; 
  @Before
  public void init() {   
    listDelegate = new UserRoleListDelegate();
    roles = new ArrayList<String>();
    roles.add("ROLE_DEV"); //$NON-NLS-1$
    roles.add("ROLE_ADMIN"); //$NON-NLS-1$
    roles.add("ROLE_DEVMGR"); //$NON-NLS-1$
    roles.add("ROLE_CEO"); //$NON-NLS-1$
    roles.add("ROLE_CTO"); //$NON-NLS-1$
    roles.add("ROLE_AUTHENTICATED"); //$NON-NLS-1$
    roles.add("ROLE_IS"); //$NON-NLS-1$
    users = new ArrayList<String>();
    users.add("pat"); //$NON-NLS-1$
    users.add("tiffany"); //$NON-NLS-1$
    users.add("joe"); //$NON-NLS-1$
    users.add("suzy"); //$NON-NLS-1$
    service = new UserDetailsRoleListService();
    listDelegate.setUserDetailsRoleListWebService(service);
    listDelegate.setUserRoleInfo(service.getUserRoleInfo());
  }
  @Test
  public void testService()  throws Exception {
    Assert.assertEquals(listDelegate.getAllUsers().size(), 4);
    Assert.assertEquals(listDelegate.getAllRoles().size(), 7);
    Assert.assertEquals(listDelegate.getUserRoleInfo().getRoles().size(), 7);
    Assert.assertEquals(listDelegate.getUserRoleInfo().getUsers().size(), 4);
  }
  
  public static class UserDetailsRoleListService implements IUserDetailsRoleListWebService  {

    public List<String> getAllRoles() {
      return roles;
    }

    public List<String> getAllUsers() {

      return users;
    }

    public UserRoleInfo getUserRoleInfo() {
      UserRoleInfo info = new UserRoleInfo();
      info.setRoles(roles);
      info.setUsers(users);
      return info;
    }
    
  }
}
