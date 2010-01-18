package org.pentaho.di.repository.pur;

import java.io.Serializable;
import java.util.Date;

import org.pentaho.di.repository.ObjectRevision;

public class PurObjectRevision implements ObjectRevision {

  // ~ Static fields/initializers ======================================================================================

  // ~ Instance fields =================================================================================================

  private String comment;

  private Date creationDate;

  private String login;

  private Serializable versionId;

  // ~ Constructors ====================================================================================================

  public PurObjectRevision(final Serializable versionId, final String login, final Date creationDate, final String comment) {
    super();
    this.versionId = versionId;
    this.login = login;
    // defensive copy
    this.creationDate = (creationDate != null ? new Date(creationDate.getTime()) : null);
    this.comment = comment;
  }

  // ~ Methods =========================================================================================================

  public String getComment() {
    return comment;
  }

  public Date getCreationDate() {
    // defensive copy
    return creationDate != null ? new Date(creationDate.getTime()) : null;
  }

  public String getLogin() {
    return login;
  }

  public String getName() {
    return versionId != null ? versionId.toString() : null;
  }

}
