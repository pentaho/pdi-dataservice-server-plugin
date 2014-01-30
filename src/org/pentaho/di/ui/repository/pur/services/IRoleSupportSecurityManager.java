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

package org.pentaho.di.ui.repository.pur.services;

import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.RepositorySecurityManager;
import org.pentaho.di.repository.pur.model.IRole;
/**
 * Repository Security Manager with the Role support
 * @author rmansoor
 *
 */
public interface IRoleSupportSecurityManager extends RepositorySecurityManager{
  /**
   * Constructs the repository version of the IRole implementation
   * 
   * @return return the instance of IRole
   * @throws KettleException 
   */
  public IRole constructRole()  throws KettleException;
  /**
   * Creates a role in the system with the given information
   * 
   * @param role to be created
   * @throws KettleException 
   */  
  public void createRole(IRole role) throws KettleException;
  /**
   * Retrieves the role with a given name
   * 
   * @param name of the role to be searched
   * @return role object matching the name 
   * @throws KettleException 
   */  
  public IRole getRole(String name) throws KettleException;

  /**
   * Retrieves all available roles in the system
   * 
   * @return the list of available roles 
   * @throws KettleException 
   */  
  public List<IRole> getRoles() throws KettleException;
  
  /**
   * Retrieves the default roles in the system.
   * 
   * @return the list of default roles
   * @throws KettleException
   */
  public List<IRole> getDefaultRoles() throws KettleException;
  
  /**
   * Save the list of roles in the system
   * 
   * @param list of role objects to be saved
   * @throws KettleException 
   */
  public void setRoles(List<IRole> roles) throws KettleException;
  /**
   * Updates a particular role in the system
   * 
   * @param role object to be updated
   * @throws KettleException 
   */
  public void updateRole(IRole role) throws KettleException;
  /**
   * Deletes a list of roles in the system
   * 
   * @param list of role object to be deleted
   * @throws KettleException 
   */
  public void deleteRoles(List<IRole> roles) throws KettleException;
  /**
   * Delete a particular role matching the role name 
   * 
   * @param name of the role to be deleted
   * @throws KettleException 
   */
  public void deleteRole(String name) throws KettleException;
}
