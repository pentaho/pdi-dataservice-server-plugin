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
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.BaseRepositorySecurityProvider;
import org.pentaho.di.repository.IUser;
import org.pentaho.di.repository.RepositorySecurityProvider;

public class PurRepositorySecurityProvider extends BaseRepositorySecurityProvider implements RepositorySecurityProvider , IUserRoleListChangeListener, java.io.Serializable {

  private static final long serialVersionUID = -1774142691342083217L; /* EESOURCE: UPDATE SERIALVERUID */

	private PurRepository	repository;
  private UserRoleListDelegate userRoleListDelegate;
  private UserRoleDelegate  userRoleDelegate;
	private static final Log logger = LogFactory.getLog(PurRepositorySecurityProvider.class);
	
  public PurRepositorySecurityProvider( PurRepository repository, PurRepositoryMeta repositoryMeta, IUser user,
      ServiceManager serviceManager ) {
    super( repositoryMeta, user );
    this.repository = repository;
    this.userRoleListDelegate = new UserRoleListDelegate( repositoryMeta, user, logger, serviceManager );
    this.setUserRoleListDelegate( userRoleListDelegate );
  }

	public PurRepository getRepository() {
		return repository;
	}
	
	public boolean isVersionCommentMandatory() {
	  return ( ( (PurRepositoryMeta) repositoryMeta ).isVersionCommentMandatory() && repository.isVersioningEnabled());
	}

	public boolean isLockingPossible() {
		return true;
	}

	public boolean isReadOnly() {
		return false;
	}

	public boolean allowsVersionComments() {
		return ( repository.isVersioningEnabled() && repository.isCommentsEnabled() );
	}

  public String[] getUserLogins() throws KettleException {
    List<String> users = userRoleListDelegate.getAllUsers();
    if(users != null && users.size() > 0) {
      String[] returnValue = new String[users.size()];
      users.toArray(returnValue);
      return returnValue;
    }
    return null;
  }


  public List<String> getAllRoles() throws KettleException {
    return userRoleListDelegate.getAllRoles();
  }

  public List<String> getAllUsers() throws KettleException {
    return userRoleListDelegate.getAllUsers();
  }

  public UserRoleDelegate getUserRoleDelegate() {
    return userRoleDelegate;
  }

  public void setUserRoleDelegate(UserRoleDelegate userRoleDelegate) {
    this.userRoleDelegate = userRoleDelegate;
    this.userRoleDelegate.addUserRoleListChangeListener(this);
  }

  public void setUserRoleListDelegate(UserRoleListDelegate userRoleListDelegate) {
    this.userRoleListDelegate = userRoleListDelegate;
  }

  public UserRoleListDelegate getUserRoleListDelegate() {
    return userRoleListDelegate;
  }

  public void onChange() {
    userRoleListDelegate.updateUserRoleList();
  }

  public static Log getLogger() {
    return logger;
  }

  @Override
  public boolean isVersioningEnabled() {
    return repository.isVersioningEnabled();
  }
}
