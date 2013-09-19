/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
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
