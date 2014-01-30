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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.pentaho.di.repository.IUser;

public class AbsRoleInfo extends EERoleInfo implements IAbsRole, java.io.Serializable {

  private static final long serialVersionUID = -4260995958866269607L; /* EESOURCE: UPDATE SERIALVERUID */

  // logical roles bound to a given runtime role
  private List<String> logicalRoles;

  public AbsRoleInfo() {
    super();
    this.logicalRoles = new ArrayList<String>();    
  }

  public AbsRoleInfo(String name, String description) {
    super(name, description);
    this.logicalRoles = new ArrayList<String>();
  }

  public AbsRoleInfo(String name, String description, Set<IUser> users, List<String> logicalRoles) {
    super(name, description, users);
    this.logicalRoles = logicalRoles;
  }

  public void addLogicalRole(String logicalRole) {
    if(logicalRoles == null) {
      this.logicalRoles = new ArrayList<String>();
    }
    if(!containsLogicalRole(logicalRole)) {
      this.logicalRoles.add(logicalRole);
    }
  }

  public void removeLogicalRole(String logicalRole) {
    if(containsLogicalRole(logicalRole)) {
      this.logicalRoles.remove(logicalRole);
    }
  }

  public List<String> getLogicalRoles() {
    return logicalRoles;
  }

  public void setLogicalRoles(List<String> logicalRoles) {
      this.logicalRoles = logicalRoles;
  }

  public boolean containsLogicalRole(String logicalRole) {
    if(logicalRoles != null) {
      for(String role:logicalRoles) {
        if(role.equals(logicalRole)) {
          return true;
        }
      }
    }
    return false;
  }
}
