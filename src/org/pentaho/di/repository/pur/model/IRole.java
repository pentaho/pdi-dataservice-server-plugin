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
 * Repository Role object
 * @author rmansoor
 *
 */
public interface IRole {
  /**
   * Set the name of the role
   * 
   * @param name of the role
   */
  public void setName(String name);
  /**
   * Retrieve the name of the role
   * 
   * @return role name
   */  
  public String getName();
  /**
   * Retrieve the role description
   * 
   * @return role name
   */    
  public String getDescription();
  /**
   * Set the description of the role
   * 
   * @param name of the role
   */
  public void setDescription(String description);
  /**
   * Associate set of users to the role
   * 
   * @param set of users
   */  
  public void setUsers(Set<IUser> users);
  /**
   * Retrieve the set of users associate to this particular role
   * 
   * @return set of associated users
   */      
  public Set<IUser> getUsers();
  /**
   * Associate a user to this particular role
   * 
   * @return status if the user association was successful
   */  
  public boolean addUser(IUser user);
  /**
   * Remove the user associate from this particular role
   * 
   * @return status if the user un association was successful 
   */    
  public boolean removeUser(IUser user);
  /**
   * Clear all the user association for this particular role
   */
  public void clearUsers();
  /**
   * Get the repository role  
   * 
   * @return repository role object 
   */    
  public IRole getRole();
}
