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

import java.util.Set;

import org.pentaho.di.ui.repository.repositoryexplorer.model.IUIUser;

public interface IUIEEUser extends IUIUser{

  public boolean addRole(IUIRole role);
  public boolean removeRole(IUIRole role);
  public void clearRoles();
  public void setRoles(Set<IUIRole> roles);
  public Set<IUIRole> getRoles();
}
