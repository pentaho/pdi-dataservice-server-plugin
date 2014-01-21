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

package org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.model;

import java.util.List;

import org.pentaho.di.repository.RepositorySecurityManager;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.IUIRole;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.IUIAbsRole;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.model.UIEESecurity;
import org.pentaho.di.ui.repository.pur.services.IAbsSecurityManager;

public class UIAbsSecurity extends UIEESecurity implements java.io.Serializable {

  private static final long serialVersionUID = -8131064658827613758L; /* EESOURCE: UPDATE SERIALVERUID */

  public UIAbsSecurity() {
    super();
  }

  public UIAbsSecurity(RepositorySecurityManager rsm) throws Exception {
    super(rsm);
    for (IUIRole systemRole : systemRoleList) {
      if (rsm instanceof IAbsSecurityManager) {
        IAbsSecurityManager asm = (IAbsSecurityManager) rsm;
        List<String> logicalRoles = asm.getLogicalRoles(systemRole.getName());
        if (systemRole instanceof IUIAbsRole) {
          ((IUIAbsRole) systemRole).setLogicalRoles(logicalRoles);
        } else {
          throw new IllegalStateException();
        }
      } else {
        throw new IllegalStateException();
      }
    }
  }

  public void addLogicalRole(String logicalRole) {
    IUIRole role = getSelectedRole();
    if (role != null) {
      if(role instanceof IUIAbsRole) {
        ((IUIAbsRole) role).addLogicalRole(logicalRole);
      } else {
        throw new IllegalStateException();
      }
    } else {
      role = getSelectedSystemRole();
      if(role instanceof IUIAbsRole) {
        ((IUIAbsRole) role).addLogicalRole(logicalRole);
      } else {
        throw new IllegalStateException();
      }
    } 
  }

  public void removeLogicalRole(String logicalRole) {
    IUIRole role = getSelectedRole();
    if (role != null) {
      if(role instanceof IUIAbsRole) {
        ((IUIAbsRole) role).removeLogicalRole(logicalRole);
      } else {
        throw new IllegalStateException();
      }
    } else {
      role = getSelectedSystemRole();
      if (role instanceof IUIAbsRole) {
        ((IUIAbsRole) role).removeLogicalRole(logicalRole);
      } else {
        throw new IllegalStateException();
      }
    }
  }
}
