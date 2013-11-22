package org.pentaho.di.repository.pur;

import java.net.MalformedURLException;

public interface ServiceManager {
  public <T> T createService( final String username, final String password, final Class<T> clazz) throws MalformedURLException;
  public void close();
}
