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

import java.util.EnumSet;

import org.pentaho.di.repository.ObjectRecipient;
import org.pentaho.platform.api.repository2.unified.RepositoryFilePermission;

public class RepositoryObjectAce implements ObjectAce, java.io.Serializable {

  private static final long serialVersionUID = 765743714498377456L; /* EESOURCE: UPDATE SERIALVERUID */

  private ObjectRecipient recipient;

  private EnumSet<RepositoryFilePermission> permissions;

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

  public RepositoryObjectAce(ObjectRecipient recipient, RepositoryFilePermission first, RepositoryFilePermission... rest) {
    this(recipient, EnumSet.of(first, rest));
  }

  public RepositoryObjectAce(ObjectRecipient recipient, EnumSet<RepositoryFilePermission> permissions) {
    this(recipient);
    this.permissions = permissions;
  }

  public ObjectRecipient getRecipient() {
    return recipient;
  }

  public EnumSet<RepositoryFilePermission> getPermissions() {
    return permissions;
  }

  public void setRecipient(ObjectRecipient recipient) {
    this.recipient = recipient;
  }

  public void setPermissions(EnumSet<RepositoryFilePermission> permissions) {
    this.permissions = permissions;
  }

  public void setPermissions(RepositoryFilePermission first, RepositoryFilePermission... rest) {
    this.permissions = EnumSet.of(first, rest);
  }

}
