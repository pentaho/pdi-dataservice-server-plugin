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
import org.pentaho.di.repository.RepositorySecurityProvider;

/**
 * Core security API for Action based security
 * 
 * <p>
 * Reponsible for determining if access to a given action should be allowed or denied.
 * 
 * @author rmansoor
 */

public interface IAbsSecurityProvider extends RepositorySecurityProvider{
  public final static String CREATE_CONTENT_ROLE = "org.pentaho.di.creator"; //$NON-NLS-1$

  public final static String READ_CONTENT_ROLE = "org.pentaho.di.reader";//$NON-NLS-1$

  public final static String ADMINISTER_SECURITY_ROLE = "org.pentaho.di.securityAdministrator";//$NON-NLS-1$

  public final static String CREATE_CONTENT_ACTION = "org.pentaho.repository.create"; //$NON-NLS-1$

  public final static String READ_CONTENT_ACTION = "org.pentaho.repository.read";//$NON-NLS-1$

  public final static String EXECUTE_CONTENT_ACTION = "org.pentaho.repository.execute";//$NON-NLS-1$

  public final static String ADMINISTER_SECURITY_ACTION = "org.pentaho.security.administerSecurity";//$NON-NLS-1$

  public final static String NAMESPACE = "org.pentaho"; //$NON-NLS-1$


  /**
  * Returns {@code true} if the the action should be allowed.
  * 
  * @param actionName name of action (e.g. {@code org.pentaho.di.repository.create})
  * @return {@code true} to allow
  */
  public boolean isAllowed(String actionName) throws KettleException;
  
  /**
  * Returns all actions in the given namespace that are currently allowed.
  * 
  * @param actionNamespace action namespace (e.g. {@code org.pentaho.di.repository}); {@code null} means all allowed actions
  * @return list of actions
  */  
  public List<String> getAllowedActions(String actionNamespace) throws KettleException;
}
