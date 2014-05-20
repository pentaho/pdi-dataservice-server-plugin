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

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.repository.ObjectRecipient;

public class RepositoryObjectAcl implements ObjectAcl, java.io.Serializable {

  private static final long serialVersionUID = 3717895033941725273L; /* EESOURCE: UPDATE SERIALVERUID */

	private List<ObjectAce> aces = new ArrayList<ObjectAce>();

	@Override
  public boolean equals(Object obj) {
	  if(obj != null) {
  	  RepositoryObjectAcl acl = (RepositoryObjectAcl) obj;
  	  if(aces != null && owner != null) {
  	    return aces.equals(acl.getAces()) && owner.equals(acl.getOwner()) && entriesInheriting == acl.isEntriesInheriting();
  	  } else if(aces == null && acl.getAces() == null) {
  	    return owner.equals(acl.getOwner()) && entriesInheriting == acl.isEntriesInheriting();
  	  } else if(owner == null && acl.getOwner() == null) {
        return aces.equals(acl.getAces()) && entriesInheriting == acl.isEntriesInheriting();
      } else {
        return false;
      }
	  } else {
	    return false;
	  }
  }

  private ObjectRecipient owner;

	private boolean entriesInheriting = true;

	// ~ Constructors
	// ====================================================================================================

	public RepositoryObjectAcl(ObjectRecipient owner) {
		this.owner = owner;
	}

	// ~ Methods
	// =========================================================================================================

	public List<ObjectAce> getAces() {
		return aces;
	}

	public ObjectRecipient getOwner() {
		return owner;
	}

	public boolean isEntriesInheriting() {
		return entriesInheriting;
	}

	public void setAces(List<ObjectAce> aces) {
		this.aces = aces;
	}

	public void setOwner(ObjectRecipient owner) {
		this.owner = owner;
	}

	public void setEntriesInheriting(boolean entriesInheriting) {
		this.entriesInheriting = entriesInheriting;
	}

  @Override
  public String toString() {
    return owner.getName();
  }
}
