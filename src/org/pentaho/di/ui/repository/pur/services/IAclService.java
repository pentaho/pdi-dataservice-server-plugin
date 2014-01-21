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

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.IRepositoryService;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.pur.model.ObjectAcl;
import org.pentaho.platform.api.repository2.unified.RepositoryFilePermission;
/**
 * Repository service which adds ACL feature to the repository. Using this feature, the user
 * of the repository can retrieve and update ACL for a particular object in the repository
 * @author rmansoor
 *
 */
public interface IAclService extends IRepositoryService{

  /**
   * Get the Permissions of a repository object.
   * 
   * @param Object Id of the repository object
   * @param forceParentInheriting retrieve the effective ACLs as if 'inherit from parent' were true
   * 
   * @return The permissions.
   * @throws KettleException in case something goes horribly wrong
   */
  public ObjectAcl getAcl(ObjectId id, boolean forceParentInheriting) throws KettleException;

  /**
   * Set the Permissions of a repository element.
   * 
   * @param Acl object that needs to be set.
   * @param Object Id of a file for which the acl are being set.
   * 
   * @throws KettleException in case something goes horribly wrong
   */
  public  void setAcl(ObjectId id, ObjectAcl aclObject) throws KettleException;
  
  public boolean hasAccess(ObjectId id, RepositoryFilePermission perm) throws KettleException;
}
