/*!
* The Pentaho proprietary code is licensed under the terms and conditions
* of the software license agreement entered into between the entity licensing
* such code and Pentaho Corporation.
*
* This software costs money - it is not free
*
* Copyright 2002 - 2013 Pentaho Corporation.  All rights reserved.
*/

package org.pentaho.di.repository.pur.model;

import java.util.HashSet;
import java.util.Set;

import org.pentaho.di.repository.UserInfo;

public class EEUserInfo extends UserInfo implements IEEUser, java.io.Serializable {

  private static final long serialVersionUID = -5327929320581502511L; /* EESOURCE: UPDATE SERIALVERUID */

  private Set<IRole> roles;
  

  public EEUserInfo() {
    super();
    this.roles = new HashSet<IRole>();
  }

  public EEUserInfo(String login, String password, String username, String description, boolean enabled,
      Set<IRole> roles) {
    super(login, password, username, description, enabled);
    this.roles = roles;
  }

  public EEUserInfo(String login, String password, String username, String description, boolean enabled) {
    super(login, password, username, description, enabled);
    this.roles = new HashSet<IRole>();
  }

  public EEUserInfo(String login) {
    super(login);
    this.roles = new HashSet<IRole>();
  }

  public EEUserInfo(EEUserInfo copyFrom) {
    super(copyFrom);
    this.roles = copyFrom.roles != null ? new HashSet<IRole>(copyFrom.roles) : null;
  }

  public boolean addRole(IRole role) {
    return this.roles.add(role);
  }

  public boolean removeRole(IRole role) {
    return this.roles.remove(role);
  }

  public void clearRoles() {
    this.roles.clear();
  }

  public void setRoles(Set<IRole> roles) {
    this.roles = roles;
  }

  public Set<IRole> getRoles() {
    return this.roles;
  }

}
