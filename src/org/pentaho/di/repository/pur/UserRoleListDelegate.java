/*!
* The Pentaho proprietary code is licensed under the terms and conditions
* of the software license agreement entered into between the entity licensing
* such code and Pentaho Corporation.
*
* This software costs money - it is not free
*
* Copyright 2002 - 2013 Pentaho Corporation.  All rights reserved.
*/

package org.pentaho.di.repository.pur;

import java.util.List;

import org.apache.commons.logging.Log;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.IUser;
import org.pentaho.platform.api.engine.security.userroledao.UserRoleInfo;
import org.pentaho.platform.security.userrole.ws.IUserRoleListWebService;

public class UserRoleListDelegate implements java.io.Serializable {

  private static final long serialVersionUID = -2895663865550206386L; /* EESOURCE: UPDATE SERIALVERUID */
  IUserRoleListWebService userDetailsRoleListWebService;
  UserRoleInfo userRoleInfo;
  Log logger;
  public UserRoleListDelegate() {
    
  }
  public UserRoleListDelegate(PurRepositoryMeta repositoryMeta, IUser userInfo, Log logger, ServiceManager serviceManager) {
    try {
      this.logger = logger;
      userDetailsRoleListWebService =
          serviceManager.createService( userInfo.getLogin(), userInfo.getPassword(), IUserRoleListWebService.class );
      updateUserRoleList();
    } catch ( Exception e ) {
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
  public IUserRoleListWebService getUserDetailsRoleListWebService() {
    return userDetailsRoleListWebService;
  }
  public void setUserDetailsRoleListWebService(IUserRoleListWebService userDetailsRoleListWebService) {
    this.userDetailsRoleListWebService = userDetailsRoleListWebService;
  }
  public UserRoleInfo getUserRoleInfo() {
    return userRoleInfo;
  }
  public void setUserRoleInfo(UserRoleInfo userRoleInfo) {
    this.userRoleInfo = userRoleInfo;
  }

}
