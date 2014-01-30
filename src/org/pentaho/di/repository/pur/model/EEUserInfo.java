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
