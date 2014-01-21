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

import java.text.SimpleDateFormat;
import java.util.Date;

import org.pentaho.di.repository.ObjectRevision;
import org.pentaho.ui.xul.XulEventSourceAdapter;

public class UIRepositoryObjectRevision extends XulEventSourceAdapter implements java.io.Serializable {

  private static final long serialVersionUID = 302159542242909806L; /* EESOURCE: UPDATE SERIALVERUID */
  
  protected ObjectRevision obj;

  public UIRepositoryObjectRevision() {
    super();
  }

  public UIRepositoryObjectRevision(ObjectRevision obj) {
    super();
    this.obj = obj;
  }
  
  public String getName() {
    return obj.getName();
  }

  public String getComment() {
    return obj.getComment();
  }

  public Date getCreationDate() {
    return obj.getCreationDate();
  }

  public String getFormatCreationDate() {
    Date date =  getCreationDate();
    String str = null;
    if (date != null){
      SimpleDateFormat sdf = new SimpleDateFormat("d MMM yyyy HH:mm:ss z");
      str = sdf.format(date);
    }
    return str;
  }

  public String getLogin() {
    return obj.getLogin();
  }
  

}
