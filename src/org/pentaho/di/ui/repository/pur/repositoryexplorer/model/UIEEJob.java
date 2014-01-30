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
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectRevision;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryElementMetaInterface;
import org.pentaho.di.repository.pur.model.EERepositoryObject;
import org.pentaho.di.repository.pur.model.ObjectAcl;
import org.pentaho.di.repository.pur.model.RepositoryLock;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.IAclObject;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.ILockObject;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.IRevisionObject;
import org.pentaho.di.ui.repository.pur.services.IAclService;
import org.pentaho.di.ui.repository.pur.services.ILockService;
import org.pentaho.di.ui.repository.pur.services.IRevisionService;
import org.pentaho.di.ui.repository.repositoryexplorer.AccessDeniedException;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIJob;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryDirectory;
import org.pentaho.platform.api.repository2.unified.RepositoryFilePermission;

public class UIEEJob extends UIJob implements ILockObject, IRevisionObject, IAclObject, java.io.Serializable {

  private static final long serialVersionUID = 1137552553918382891L; /* EESOURCE: UPDATE SERIALVERUID */
  private ILockService lockService;
  private IRevisionService revisionService;
  private IAclService aclService;
  private UIRepositoryObjectRevisions revisions;
  private EERepositoryObject repObj;
  private ObjectAcl acl;
  private Map<RepositoryFilePermission,Boolean> hasAccess = null;

  public UIEEJob(RepositoryElementMetaInterface rc, UIRepositoryDirectory parent, Repository rep) {
    super(rc, parent, rep);
    if (!(rc instanceof EERepositoryObject)) {
      throw new IllegalArgumentException();
    }
    repObj = (EERepositoryObject) rc;
    try {
      if (rep.hasService(ILockService.class)) {
        lockService = (ILockService) rep.getService(ILockService.class);
      } else {
        throw new IllegalStateException();
      }
      if (rep.hasService(IRevisionService.class)) {
        revisionService = (IRevisionService) rep.getService(IRevisionService.class);
      } else {
        throw new IllegalStateException();
      }
      if (rep.hasService(IAclService.class)) {
        aclService = (IAclService) rep.getService(IAclService.class);
      } else {
        throw new IllegalStateException();
      }      
    } catch (KettleException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getImage() {
    try {
      if(isLocked()) {
        return "images/lock.png"; //$NON-NLS-1$
      }
    } catch (KettleException e) {
      throw new RuntimeException(e);
    }
    return "images/job.png"; //$NON-NLS-1$
  }

  public String getLockMessage() throws KettleException {
    return repObj.getLockMessage();
  }

  public void lock(String lockNote) throws KettleException {
    RepositoryLock lock = lockService.lockJob(getObjectId(), lockNote);
    repObj.setLock(lock);
    uiParent.fireCollectionChanged();
  }

  public void unlock() throws KettleException {
    lockService.unlockJob(getObjectId());
    repObj.setLock(null);
    uiParent.fireCollectionChanged();
  }
  
  public boolean isLocked() throws KettleException {
    return (getRepositoryLock() != null);
  }
  
  public RepositoryLock getRepositoryLock() throws KettleException {
    return repObj.getLock();
  }
  
  public UIRepositoryObjectRevisions getRevisions() throws KettleException {
    if (revisions != null){
      return revisions;
    }
    
    revisions = new UIRepositoryObjectRevisions();
    
    List <ObjectRevision> or = revisionService.getRevisions(getObjectId());

    for (ObjectRevision rev : or) {
      revisions.add(new UIRepositoryObjectRevision(rev));
    }
    return revisions;
  }
  
  protected void refreshRevisions() throws KettleException {
    revisions = null;
    getRevisions();
  }
  
  
  public void restoreRevision(UIRepositoryObjectRevision revision, String commitMessage) throws KettleException {
    if(revisionService != null) {
      revisionService.restoreJob(this.getObjectId(), revision.getName(), commitMessage);
      refreshRevisions();
      uiParent.fireCollectionChanged();
    }
  }
  
  public void getAcls(UIRepositoryObjectAcls acls, boolean forceParentInheriting) throws AccessDeniedException{
    if (acl == null) {
      try {
        acl = aclService.getAcl(getObjectId(), forceParentInheriting);
      } catch(KettleException ke) {
        throw new AccessDeniedException(ke);
      }
    }
    acls.setObjectAcl(acl);
  }

  public void getAcls(UIRepositoryObjectAcls acls) throws AccessDeniedException{
    getAcls(acls, false);
  }

  public void setAcls(UIRepositoryObjectAcls security) throws AccessDeniedException{
    try {
      aclService.setAcl(getObjectId(), security.getObjectAcl());
    } catch (KettleException e) {
      throw new AccessDeniedException(e);
    }
  }

  @Override
  public void clearAcl() {
    acl = null;
    hasAccess = null;
  }

  @Override
  public boolean hasAccess(RepositoryFilePermission perm) throws KettleException {
    if (hasAccess == null) {
      hasAccess = new HashMap<RepositoryFilePermission, Boolean>();
    }
    if (hasAccess.get(perm) == null) {
      hasAccess.put(perm, new Boolean(aclService.hasAccess(repObj.getObjectId(), perm)));
    }
    return hasAccess.get(perm).booleanValue();
  }
}
