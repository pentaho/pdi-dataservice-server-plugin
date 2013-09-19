/*!
* The Pentaho proprietary code is licensed under the terms and conditions
* of the software license agreement entered into between the entity licensing
* such code and Pentaho Corporation.
*
* This software costs money - it is not free
*
* Copyright 2002 - 2013 Pentaho Corporation.  All rights reserved.
*/

package org.pentaho.di.repository.pur.model;

import java.util.List;

import org.pentaho.di.repository.ObjectRecipient;


public interface ObjectAcl {

	public List<ObjectAce> getAces();
	public ObjectRecipient getOwner();
	public boolean isEntriesInheriting();
	public void setAces(List<ObjectAce> aces);
	public void setOwner(ObjectRecipient owner);
	public void setEntriesInheriting(boolean entriesInheriting);
}
