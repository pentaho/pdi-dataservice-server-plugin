/*!
* The Pentaho proprietary code is licensed under the terms and conditions
* of the software license agreement entered into between the entity licensing
* such code and Pentaho Corporation.
*
* This software costs money - it is not free
*
* Copyright 2002 - 2013 Pentaho Corporation.  All rights reserved.
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
