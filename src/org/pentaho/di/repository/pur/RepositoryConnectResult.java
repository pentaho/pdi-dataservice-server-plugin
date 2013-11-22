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
