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

import java.util.Set;

import org.pentaho.di.repository.IUser;

/**
 * Repository User object with role support
 * @author rmansoor
 *
 */
public interface IEEUser extends IUser {
  /**
   * Associate a role to this particular user
   * 
   * @param role to be associate
   * @return return the status whether the role association to this user was successful or not
   */
  public boolean addRole(IRole role);
  /**
   * Remove the association of a role to this particular user
   * 
   * @param role to be un associated
   * @return return the status whether the role un association to this user was successful or not
   */  
  public boolean removeRole(IRole role);
  /**
   * Clear all the role association from this particular user
   * 
   */    
  public void clearRoles();
  /**
   * Associate set of roles to this particular user
   * 
   * @param set of roles
   */  
  public void setRoles(Set<IRole> roles);
  /**
   * Retrieve the set of roles associated to this particular user
   * 
   * @return set of associated roles
   */      
  public Set<IRole> getRoles();
}
