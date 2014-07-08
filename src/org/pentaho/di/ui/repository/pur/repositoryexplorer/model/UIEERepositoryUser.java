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

package org.pentaho.di.ui.repository.pur.repositoryexplorer.model;

import java.util.HashSet;
import java.util.Set;

import org.pentaho.di.repository.IUser;
import org.pentaho.di.repository.pur.model.IEEUser;
import org.pentaho.di.repository.pur.model.IRole;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.IUIEEUser;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.IUIRole;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.UIEEObjectRegistery;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIObjectCreationException;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryUser;

public class UIEERepositoryUser extends UIRepositoryUser implements IUIEEUser, java.io.Serializable {

  private static final long serialVersionUID = -4653578043082025692L; /* EESOURCE: UPDATE SERIALVERUID */
  private IEEUser eeUser;

  public UIEERepositoryUser() {
    super();
    // TODO Auto-generated constructor stub
  }

  public UIEERepositoryUser( IUser user ) {
    super( user );
    if ( user instanceof IEEUser ) {
      eeUser = (IEEUser) user;
    }
  }

  public boolean addRole( IUIRole role ) {
    return eeUser.addRole( role.getRole() );
  }

  public boolean removeRole( IUIRole role ) {
    return removeRole( role.getRole().getName() );
  }

  public void clearRoles() {
    eeUser.clearRoles();
  }

  public void setRoles( Set<IUIRole> roles ) {
    Set<IRole> roleSet = new HashSet<IRole>();
    for ( IUIRole role : roles ) {
      roleSet.add( role.getRole() );
    }
    eeUser.setRoles( roleSet );
  }

  public Set<IUIRole> getRoles() {
    Set<IUIRole> rroles = new HashSet<IUIRole>();
    for ( IRole role : eeUser.getRoles() ) {
      try {
        rroles.add( UIEEObjectRegistery.getInstance().constructUIRepositoryRole( role ) );
      } catch ( UIObjectCreationException uex ) {

      }
    }
    return rroles;
  }

  private boolean removeRole( String roleName ) {
    IRole roleInfo = null;
    for ( IRole role : eeUser.getRoles() ) {
      if ( role.getName().equals( roleName ) ) {
        roleInfo = role;
        break;
      }
    }
    if ( roleInfo != null ) {
      return eeUser.removeRole( roleInfo );
    } else {
      return false;
    }
  }

  @Override public String toString() {
    return eeUser.getLogin();
  }
}
