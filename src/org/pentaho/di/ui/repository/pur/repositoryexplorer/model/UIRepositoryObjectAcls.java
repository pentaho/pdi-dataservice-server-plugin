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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.pentaho.di.repository.ObjectRecipient;
import org.pentaho.di.repository.pur.model.ObjectAce;
import org.pentaho.di.repository.pur.model.ObjectAcl;
import org.pentaho.platform.api.repository2.unified.RepositoryFilePermission;
import org.pentaho.ui.xul.XulEventSourceAdapter;

/**
 * TODO mlowery This class represents an ACL, not an ACLs.
 */
public class UIRepositoryObjectAcls extends XulEventSourceAdapter implements java.io.Serializable {

  private static final long serialVersionUID = -4576328356619980808L; /* EESOURCE: UPDATE SERIALVERUID */

  protected ObjectAcl obj;

  private List<UIRepositoryObjectAcl> selectedAclList = new ArrayList<UIRepositoryObjectAcl>();

  private boolean removeEnabled;

  private boolean modelDirty;
  
  private boolean hasManageAclAccess;

  public UIRepositoryObjectAcls() {
    super();
  }

  // ~ Methods
  // =========================================================================================================

  public void setObjectAcl(ObjectAcl obj) {
    this.obj = obj;
    this.firePropertyChange("acls", null, getAcls()); //$NON-NLS-1$
    this.firePropertyChange("entriesInheriting", null, isEntriesInheriting()); //$NON-NLS-1$
  }

  public ObjectAcl getObjectAcl() {
    return this.obj;
  }

  public List<UIRepositoryObjectAcl> getAcls() {
    if (obj != null) {
      List<UIRepositoryObjectAcl> acls = new ArrayList<UIRepositoryObjectAcl>();
      for (ObjectAce ace : obj.getAces()) {
        acls.add(new UIRepositoryObjectAcl(ace));
      }
      return acls;
    }
    return null;
  }

  public void setAcls(List<UIRepositoryObjectAcl> acls) {
    List<UIRepositoryObjectAcl> prevousVal = new ArrayList<UIRepositoryObjectAcl>();
    prevousVal.addAll(getAcls());

    this.obj.getAces().clear();
    if (acls != null) {
      for (UIRepositoryObjectAcl acl : acls) {
        obj.getAces().add(acl.getAce());
      }
    }
    this.firePropertyChange("acls", prevousVal, getAcls()); //$NON-NLS-1$
  }

  public void addAcls(List<UIRepositoryObjectAcl> aclsToAdd) {
    for (UIRepositoryObjectAcl acl : aclsToAdd) {
      addAcl(acl);
    }
    this.firePropertyChange("acls", null, getAcls()); //$NON-NLS-1$
    // Setting the selected index to the first item in the list
    if(obj.getAces().size() > 0) {
      List<UIRepositoryObjectAcl> aclList = new ArrayList<UIRepositoryObjectAcl>();
      aclList.add(new UIRepositoryObjectAcl(getAceAtIndex(0)));
      setSelectedAclList(aclList);
    }
    setRemoveEnabled(!obj.isEntriesInheriting() && !isEmpty() && hasManageAclAccess());
    setModelDirty(true);
  }
  public void addAcl(UIRepositoryObjectAcl aclToAdd) {
    // By default the user or role will get a READ, READ_ACL when a user of role is added
    EnumSet<RepositoryFilePermission> initialialPermisson = EnumSet.of(RepositoryFilePermission.READ);
    aclToAdd.setPermissionSet(initialialPermisson);
    this.obj.getAces().add(aclToAdd.getAce());
  }

  public void removeAcls(List<UIRepositoryObjectAcl> aclsToRemove) {
    for (UIRepositoryObjectAcl acl : aclsToRemove) {
      removeAcl(acl.getRecipientName());
    }

    this.firePropertyChange("acls", null, getAcls()); //$NON-NLS-1$
    if(obj.getAces().size() > 0) {
      List<UIRepositoryObjectAcl> aclList = new ArrayList<UIRepositoryObjectAcl>();
      aclList.add(new UIRepositoryObjectAcl(getAceAtIndex(0)));
      setSelectedAclList(aclList);
    } else {
      setSelectedAclList(null);
    }
    setRemoveEnabled(!obj.isEntriesInheriting() && !isEmpty() && hasManageAclAccess());
    setModelDirty(true);
  }

  public void removeAcl(String recipientName) {
    ObjectAce aceToRemove = null;

    for (ObjectAce ace : obj.getAces()) {
      if (ace.getRecipient().getName().equals(recipientName)) {
        aceToRemove = ace;
        break;
      }
    }
    obj.getAces().remove(aceToRemove);
  }

  public void removeSelectedAcls() {
    // side effect deletes multiple acls when only one selected.
    List<UIRepositoryObjectAcl> removalList = new ArrayList<UIRepositoryObjectAcl>();
    for (UIRepositoryObjectAcl rem : getSelectedAclList()){
      removalList.add(rem);
    }
    removeAcls(removalList);
  }
  public void updateAcl(UIRepositoryObjectAcl aclToUpdate) {
    List<ObjectAce> aces = obj.getAces();
    for (ObjectAce ace : aces) {
      if (ace.getRecipient().getName().equals(aclToUpdate.getRecipientName())) {
        ace.setPermissions(aclToUpdate.getPermissionSet());
      }
    }
    UIRepositoryObjectAcl acl = getAcl(aclToUpdate.getRecipientName());
    acl.setPermissionSet(aclToUpdate.getPermissionSet());
    this.firePropertyChange("acls", null, getAcls()); //$NON-NLS-1$
    
    // above firePropertyChange replaces all elements in the listBox and therefore clears any selected elements; 
    //   however, the selectedAclList field is never updated because no selectedIndices event is ever called; manually
    //   update it to reflect the selected state of the user/role list now (no selection)
    selectedAclList.clear();
    
    // Setting the selected index
    List<UIRepositoryObjectAcl> aclList = new ArrayList<UIRepositoryObjectAcl>();
    aclList.add(aclToUpdate);
    setSelectedAclList(aclList);

    setModelDirty(true);
  }

  public UIRepositoryObjectAcl getAcl(String recipient) {
    for (ObjectAce ace : obj.getAces()) {
      if (ace.getRecipient().getName().equals(recipient)) {
        return new UIRepositoryObjectAcl(ace);
      }
    }
    return null;
  }

  public List<UIRepositoryObjectAcl> getSelectedAclList() {
    return selectedAclList;
  }

  public void setSelectedAclList(List<UIRepositoryObjectAcl> list) {
    if(this.selectedAclList != null && this.selectedAclList.equals(list)) {
      return;
    }

    List<UIRepositoryObjectAcl> previousVal = new ArrayList<UIRepositoryObjectAcl>();
    previousVal.addAll(selectedAclList);
    selectedAclList.clear();
    if(list != null) {
      selectedAclList.addAll(list);
      this.firePropertyChange("selectedAclList", previousVal, list); //$NON-NLS-1$
    }
    setRemoveEnabled(!isEntriesInheriting() && !isEmpty() && hasManageAclAccess());
  }

  public boolean isEntriesInheriting() {
    if (obj != null) {
      return obj.isEntriesInheriting();
    } else
      return false;
  }

  public void setEntriesInheriting(boolean entriesInheriting) {
    if (obj != null) {
      boolean previousVal = isEntriesInheriting();
      obj.setEntriesInheriting(entriesInheriting);
      this.firePropertyChange("entriesInheriting", previousVal, entriesInheriting); //$NON-NLS-1$
      setSelectedAclList(null);
      setRemoveEnabled(!entriesInheriting && !isEmpty() && hasManageAclAccess());
      // Only dirty the model if the value has changed
      if(previousVal != entriesInheriting)
    	  setModelDirty(true);
    }
  }

  public ObjectRecipient getOwner() {
    if (obj != null) {
      return obj.getOwner();
    } else {
      return null;
    }
  }

  public void setRemoveEnabled(boolean removeEnabled) {
    this.removeEnabled = removeEnabled;
    this.firePropertyChange("removeEnabled", null, removeEnabled); //$NON-NLS-1$
  }

  public boolean isRemoveEnabled() {
    return removeEnabled;
  }

  public int getAceIndex(ObjectAce ace) {
    List<ObjectAce> aceList = obj.getAces();
    for (int i = 0; i < aceList.size(); i++) {
      if (ace.equals(aceList.get(i))) {
        return i;
      }
    }
    return -1;
  }

  public ObjectAce getAceAtIndex(int index) {
    if (index >= 0) {
      return obj.getAces().get(index);
    } else {
      return null;
    }
  }

  public void setModelDirty(boolean modelDirty) {
    this.modelDirty = modelDirty;
  }

  public boolean isModelDirty() {
    return modelDirty;
  }
  
  public boolean hasManageAclAccess() {
    return hasManageAclAccess;
  }
  
  public void setHasManageAclAccess(boolean hasManageAclAccess) {
    this.hasManageAclAccess = hasManageAclAccess;
  }
  
  public void clear() {
    setRemoveEnabled(false);
    setModelDirty(false);
    setAcls(null);
    setSelectedAclList(null);
    setHasManageAclAccess(false);
  }
  
  private boolean isEmpty() {
    return getSelectedAclList() == null || getSelectedAclList().size() <= 0;
  }
}
