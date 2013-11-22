package org.pentaho.di.repository.pur;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleSecurityException;

public interface IRepositoryConnector {
  public RepositoryConnectResult connect( final String username, final String password ) throws KettleException,
    KettleSecurityException;
  public void disconnect();
}
