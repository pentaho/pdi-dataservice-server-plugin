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

import java.util.List;
/**
 * Repository Role with ABS support
 * @author rmansoor
 *
 */
public interface IAbsRole extends IRole{

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
