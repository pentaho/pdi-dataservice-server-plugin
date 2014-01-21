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

import org.pentaho.di.repository.IUser;
import org.pentaho.di.repository.RepositorySecurityManager;
import org.pentaho.di.repository.RepositorySecurityProvider;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;

public class RepositoryConnectResult {
  private final RepositoryServiceRegistry repositoryServiceRegistry;
  private boolean success;
  private IUser user;
  private IUnifiedRepository unifiedRepository;
  private RepositorySecurityManager securityManager;
  private RepositorySecurityProvider securityProvider;
  private String connectMessage;

  public RepositoryConnectResult( RepositoryServiceRegistry repositoryServiceRegistry ) {
    this.repositoryServiceRegistry = repositoryServiceRegistry;
  }

  public RepositoryServiceRegistry repositoryServiceRegistry() {
    return repositoryServiceRegistry;
  }

  public boolean isSuccess() {
    return success;
  }

  public IUser getUser() {
    return user;
  }

  public IUnifiedRepository getUnifiedRepository() {
    return unifiedRepository;
  }

  public void setSuccess( boolean success ) {
    this.success = success;
  }

  public void setUser( IUser user ) {
    this.user = user;
  }

  public void setUnifiedRepository( IUnifiedRepository unifiedRepository ) {
    this.unifiedRepository = unifiedRepository;
  }

  public RepositorySecurityManager getSecurityManager() {
    return securityManager;
  }

  public void setSecurityManager( RepositorySecurityManager securityManager ) {
    this.securityManager = securityManager;
  }

  public RepositorySecurityProvider getSecurityProvider() {
    return securityProvider;
  }

  public void setSecurityProvider( RepositorySecurityProvider securityProvider ) {
    this.securityProvider = securityProvider;
  }

  public String getConnectMessage() {
    return connectMessage;
  }

  public void setConnectMessage( String connectMessage ) {
    this.connectMessage = connectMessage;
  }
}
