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
import java.util.Map;

import org.pentaho.di.core.exception.KettleException;

/**
 * This is the Admin API for Action based security. Bind logical role(s) to runtime role
 * 
 * @author rmansoor
 */

public interface IAbsSecurityManager extends IRoleSupportSecurityManager{


  /**
  * Initialize the service and get the role binding struct
  * 
  * @param locale 
  * @throws KettleException
  */
  public void initialize(String locale)  throws KettleException;
  /**
  * Sets the bindings for the given runtime role. All other bindings for this runtime role are removed.
  * 
  * @param runtimeRoleName runtime role name
  * @param logicalRoleNames list of logical role names
  * @throws KettleException
  */
  public void setLogicalRoles(String rolename , List<String> logicalRoles)  throws KettleException;
  /**
  * Get all the logical role names for the given runtime role. 
  * 
  * @param runtimeRole 
  * @return list of logical roles
  * @throws KettleException
  */
  public List<String> getLogicalRoles(String runtimeRole)  throws KettleException;

  /**
  * Get the localized logical role names for the given locale. 
  * 
  * @param locale 
  * @return map of localized logical roles
  * @throws KettleException
  */
  public Map<String, String> getAllLogicalRoles(String locale)  throws KettleException;
}
