package org.pentaho.di.repository;

import java.util.List;

import org.pentaho.di.core.exception.KettleException;

/**
 * Core security API for Action based security
 * 
 * <p>
 * Reponsible for determining if access to a given action should be allowed or denied.
 * 
 * @author rmansoor
 */

public interface IAbsSecurityProvider {
  
  /**
  * Returns {@code true} if the the action should be allowed.
  * 
  * @param actionName name of action (e.g. {@code org.pentaho.di.repository.create})
  * @return {@code true} to allow
  */
  public boolean isAllowed(String actionName) throws KettleException;
  
  /**
  * Returns all actions in the given namespace that are currently allowed.
  * 
  * @param actionNamespace action namespace (e.g. {@code org.pentaho.di.repository}); {@code null} means all allowed actions
  * @return list of actions
  */  
  public List<String> getAllowedActions(String actionNamespace) throws KettleException;
}
