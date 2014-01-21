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

import java.util.HashMap;
import java.util.Map;

import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.IAclObject;
import org.pentaho.di.ui.repository.pur.services.IAclService;
import org.pentaho.di.ui.repository.repositoryexplorer.AccessDeniedException;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIDatabaseConnection;
import org.pentaho.platform.api.repository2.unified.RepositoryFilePermission;

/**
 * This UI DB Connection extends the default and allows for ACLs in the view
 * 
 * @author Will Gorman (wgorman@pentaho.com)
 *
 */
public class UIEEDatabaseConnection extends UIDatabaseConnection implements IAclObject {
  private IAclService aclService;
  private Map<RepositoryFilePermission,Boolean> hasAccess = null;

  
  public UIEEDatabaseConnection() {
    super();
  }

  public UIEEDatabaseConnection(DatabaseMeta meta, Repository rep) {
    super(meta, rep);
    initializeService(rep);
  }

  private void initializeService(Repository rep) {
    try {
      if (rep.hasService(IAclService.class)) {
        aclService = (IAclService) rep.getService(IAclService.class);
      } else {
        throw new IllegalStateException();
      }
    } catch (KettleException e) {
      throw new RuntimeException(e);
    } 

  }
  
  public void getAcls(UIRepositoryObjectAcls acls, boolean forceParentInheriting) throws AccessDeniedException{
    try {
      acls.setObjectAcl(aclService.getAcl(getDatabaseMeta().getObjectId(), forceParentInheriting));
    } catch(KettleException ke) {
      throw new AccessDeniedException(ke);
    }
  }

  public void getAcls(UIRepositoryObjectAcls acls) throws AccessDeniedException{
    try {
      acls.setObjectAcl(aclService.getAcl(getDatabaseMeta().getObjectId(), false));
    } catch(KettleException ke) {
      throw new AccessDeniedException(ke);
    }
  }

  public void setAcls(UIRepositoryObjectAcls security) throws AccessDeniedException{
    try {
      aclService.setAcl(getDatabaseMeta().getObjectId(), security.getObjectAcl());
    } catch (KettleException e) {
      throw new AccessDeniedException(e);
    }
  }

  @Override
  public void clearAcl() {
    hasAccess = null;
  }

  @Override
  public boolean hasAccess(RepositoryFilePermission perm) throws KettleException {
    if (hasAccess == null) {
      hasAccess = new HashMap<RepositoryFilePermission, Boolean>();
    }
    if (hasAccess.get(perm) == null) {
      hasAccess.put(perm, new Boolean(aclService.hasAccess(getDatabaseMeta().getObjectId(), perm)));
    }
    return hasAccess.get(perm).booleanValue();
  }
}
