package org.pentaho.di.repository.pur.model;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.repository.ObjectRecipient;

public class RepositoryObjectAcl implements ObjectAcl{

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
}
