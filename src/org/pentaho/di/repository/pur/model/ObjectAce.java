/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.repository.pur.model;

import java.util.EnumSet;

import org.pentaho.di.repository.ObjectRecipient;

public interface ObjectAce {

    public ObjectRecipient getRecipient();
    public EnumSet<ObjectPermission> getPermissions();
  	public void setRecipient(ObjectRecipient recipient);
  	public void setPermissions(ObjectPermission first, ObjectPermission... rest);
  	public void setPermissions(EnumSet<ObjectPermission> permissions);
}
