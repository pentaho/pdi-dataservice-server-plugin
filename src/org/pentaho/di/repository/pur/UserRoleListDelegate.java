/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
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
