package org.pentaho.di.repository.pur.model;

import java.util.EnumSet;

import org.pentaho.di.repository.ObjectRecipient;

public class RepositoryObjectAce implements ObjectAce {

  private ObjectRecipient recipient;

  private EnumSet<ObjectPermission> permissions;

  @Override
  public boolean equals(Object obj) {
    if (obj != null) {
      RepositoryObjectAce ace = (RepositoryObjectAce) obj;

      if (recipient == null && permissions == null && ace.getRecipient() == null && ace.getPermissions() == null) {
        return true;
      } else if (recipient != null && permissions != null) {
        return recipient.equals(ace.getRecipient()) && permissions.equals(ace.getPermissions());
      } else if (ace.getRecipient() != null && ace.getPermissions() != null ){
        return ace.getRecipient().equals(recipient) && ace.getPermissions().equals(permissions);
      } else if (ace.getPermissions() == null && permissions == null) {
        return recipient.equals(ace.getRecipient());
      } else if (ace.getRecipient() == null && recipient == null) {
        return permissions.equals(ace.getPermissions());
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  public RepositoryObjectAce(ObjectRecipient recipient) {
    this.recipient = recipient;
  }

  public RepositoryObjectAce(ObjectRecipient recipient, ObjectPermission first, ObjectPermission... rest) {
    this(recipient, EnumSet.of(first, rest));
  }

  public RepositoryObjectAce(ObjectRecipient recipient, EnumSet<ObjectPermission> permissions) {
    this(recipient);
    this.permissions = permissions;
  }

  public ObjectRecipient getRecipient() {
    return recipient;
  }

  public EnumSet<ObjectPermission> getPermissions() {
    return permissions;
  }

  public void setRecipient(ObjectRecipient recipient) {
    this.recipient = recipient;
  }

  public void setPermissions(EnumSet<ObjectPermission> permissions) {
    this.permissions = permissions;
  }

  public void setPermissions(ObjectPermission first, ObjectPermission... rest) {
    this.permissions = EnumSet.of(first, rest);
  }

}
