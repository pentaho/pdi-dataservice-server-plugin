package org.pentaho.di.ui.repository.pur.repositoryexplorer.model;

import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectRevision;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryContent;
import org.pentaho.di.repository.pur.model.RepositoryLock;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.IAclObject;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.ILockObject;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.IRevisionObject;
import org.pentaho.di.ui.repository.pur.services.IAclService;
import org.pentaho.di.ui.repository.pur.services.ILockService;
import org.pentaho.di.ui.repository.pur.services.IRevisionService;
import org.pentaho.di.ui.repository.repositoryexplorer.AccessDeniedException;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryDirectory;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UITransformation;

public class UIEETransformation extends UITransformation implements ILockObject, IRevisionObject, IAclObject {
  private ILockService lockService;
  private IAclService aclService;
  private IRevisionService revisionService;
  private UIRepositoryObjectRevisions revisions;

  public UIEETransformation(RepositoryContent rc, UIRepositoryDirectory parent, Repository rep) {
    super(rc, parent, rep);
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
      if (isLocked()) {
        return "images/lock.png"; //$NON-NLS-1$
      }
    } catch (KettleException e) {
      throw new RuntimeException(e);
    }
    return "images/transformation.png"; //$NON-NLS-1$
  }

  public String getLockMessage() throws KettleException {
    String result = null;
    RepositoryLock objLock = getRepositoryLock();
    if (objLock != null) {
      result = objLock.getMessage();
    }
    return result;
  }

  public void lock(String lockNote) throws KettleException {
    lockService.lockTransformation(getObjectId(), lockNote);
    refreshRevisions();
    uiParent.fireCollectionChanged();
  }

  public void unlock() throws KettleException {
    lockService.unlockTransformation(getObjectId());
    refreshRevisions();
    uiParent.fireCollectionChanged();
  }

  public boolean isLocked() throws KettleException {
    return (getRepositoryLock() != null);
  }

  public RepositoryLock getRepositoryLock() throws KettleException {
    return lockService.getTransformationLock(getObjectId());
  }

  public UIRepositoryObjectRevisions getRevisions() throws KettleException {
    if (revisions != null) {
      return revisions;
    }

    revisions = new UIRepositoryObjectRevisions();

    List<ObjectRevision> or = revisionService.getRevisions(getObjectId());

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
    if (revisionService != null) {
      revisionService.restoreTransformation(this.getObjectId(), revision.getName(), commitMessage);
      refreshRevisions();
      uiParent.fireCollectionChanged();
    }
  }
  public void getAcls(UIRepositoryObjectAcls acls, boolean forceParentInheriting) throws AccessDeniedException{
    try {
      acls.setObjectAcl(aclService.getAcl(getObjectId(), forceParentInheriting));
    } catch(KettleException ke) {
      throw new AccessDeniedException(ke);
    }
  }

  public void getAcls(UIRepositoryObjectAcls acls) throws AccessDeniedException{
    try {
      acls.setObjectAcl(aclService.getAcl(getObjectId(), false));
    } catch(KettleException ke) {
      throw new AccessDeniedException(ke);
    }
  }

  public void setAcls(UIRepositoryObjectAcls security) throws AccessDeniedException{
    try {
      aclService.setAcl(getObjectId(), security.getObjectAcl());
    } catch (KettleException e) {
      throw new AccessDeniedException(e);
    }
  }
}
