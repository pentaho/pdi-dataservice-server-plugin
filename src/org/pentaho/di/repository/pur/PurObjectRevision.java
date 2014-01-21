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

package org.pentaho.di.repository.pur;

import java.io.Serializable;
import java.util.Date;

import org.pentaho.di.repository.ObjectRevision;

public class PurObjectRevision implements ObjectRevision, java.io.Serializable {

  private static final long serialVersionUID = -7857510728831225268L; /* EESOURCE: UPDATE SERIALVERUID */

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
