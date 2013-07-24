/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.ui.repository.pur.repositoryexplorer;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.model.UIRepositoryObjectAcls;
import org.pentaho.di.ui.repository.repositoryexplorer.AccessDeniedException;
import org.pentaho.platform.api.repository2.unified.RepositoryFilePermission;

public interface IAclObject {

  public void getAcls(UIRepositoryObjectAcls acls) throws AccessDeniedException;

  public void getAcls(UIRepositoryObjectAcls acls, boolean forceParentInheriting) throws AccessDeniedException;

  public void setAcls(UIRepositoryObjectAcls security) throws AccessDeniedException;
  
  /**
   * Clear the cached ACL so it is refreshed upon next request.
   */
  public void clearAcl();
  
  public boolean hasAccess(RepositoryFilePermission perm) throws KettleException;
}
