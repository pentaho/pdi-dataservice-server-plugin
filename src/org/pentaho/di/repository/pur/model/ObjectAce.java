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
