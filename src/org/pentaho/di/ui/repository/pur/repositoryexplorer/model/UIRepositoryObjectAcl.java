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

import java.util.EnumSet;

import org.pentaho.di.repository.ObjectRecipient;
import org.pentaho.di.repository.pur.model.ObjectAce;
import org.pentaho.platform.api.repository2.unified.RepositoryFilePermission;
import org.pentaho.ui.xul.XulEventSourceAdapter;

/**
 * TODO mlowery This class represents an ACE, not an ACL.
 */
public class UIRepositoryObjectAcl extends XulEventSourceAdapter implements java.io.Serializable {

  private static final long serialVersionUID = 8320176731576605496L; /* EESOURCE: UPDATE SERIALVERUID */
	
	@Override
  public boolean equals(Object obj) {
	  if(obj == null) {
	    return false;
	  }
	  UIRepositoryObjectAcl acl = (UIRepositoryObjectAcl) obj;
    return ace.equals(acl.getAce());
  }
  protected ObjectAce ace;
	
	public ObjectAce getAce() {
    return ace;
  }
  public UIRepositoryObjectAcl(ObjectAce ace) {
		this.ace = ace;
	}
	public String getRecipientName() {
		return ace.getRecipient().getName();
	}
	public void setRecipientName(String recipientName) {
		ace.getRecipient().setName(recipientName);		
		this.firePropertyChange("recipientName", null, recipientName); //$NON-NLS-1$
	}
	public ObjectRecipient.Type getRecipientType() {
		return ace.getRecipient().getType();
	}
	public void setRecipientType(ObjectRecipient.Type recipientType) {
		ace.getRecipient().setType(recipientType);
		this.firePropertyChange("recipientType", null, recipientType); //$NON-NLS-1$		
	}
	public EnumSet<RepositoryFilePermission> getPermissionSet() {
		return ace.getPermissions();
	}
	public void setPermissionSet(RepositoryFilePermission first, RepositoryFilePermission... rest) {
		ace.setPermissions(first, rest);
		this.firePropertyChange("permissions", null, ace.getPermissions()); //$NON-NLS-1$
	}
	
	public void setPermissionSet(EnumSet<RepositoryFilePermission> permissionSet) {
		EnumSet<RepositoryFilePermission> previousVal = ace.getPermissions(); 
		ace.setPermissions(permissionSet);
		this.firePropertyChange("permissions", previousVal, ace.getPermissions()); //$NON-NLS-1$
	}
	
	public void addPermission(RepositoryFilePermission permissionToAdd) {
		ace.getPermissions().add(permissionToAdd);
	}
	public void removePermission(RepositoryFilePermission permissionToRemove) {
		ace.getPermissions().remove(permissionToRemove);
	}
}
