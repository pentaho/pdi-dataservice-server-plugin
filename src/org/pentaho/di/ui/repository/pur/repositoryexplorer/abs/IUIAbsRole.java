/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.ui.repository.pur.repositoryexplorer.abs;

import java.util.List;

import org.pentaho.di.ui.repository.pur.repositoryexplorer.IUIRole;


public interface IUIAbsRole extends IUIRole{
  /**
   * Associate a logical role to the runtime role
   * 
   * @param logical role name to be associated
   */  
  public void addLogicalRole(String logicalRole);
  /**
   * Remove the logical role association from this particular runtime role
   * 
   * @param logical role name to be un associated
   */  
  public void removeLogicalRole(String logicalRole);
  /**
   * Check whether a logical role is associated to this runtime role
   * 
   * @param logical role name to be checked
   */    
  public boolean containsLogicalRole(String logicalRole);
  /**
   * Associate set of logical roles to this particular runtime role
   * 
   * @param list of logical role name
   */   
  public void setLogicalRoles(List<String> logicalRoles);
  /**
   * Retrieve the list of roles association for this particular runtime role
   * 
   * @return list of associated roles
   */ 
  public List<String> getLogicalRoles();

}
