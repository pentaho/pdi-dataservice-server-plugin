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

package org.pentaho.di.ui.repository.pur.model;

import org.pentaho.ui.xul.XulEventSourceAdapter;

/**
 * @author rmansoor
 *
 */
public class RepositoryConfigModel extends XulEventSourceAdapter implements java.io.Serializable {

  private static final long serialVersionUID = 1018425117620046943L; /* EESOURCE: UPDATE SERIALVERUID */
  private String url;
  private String id;
  private String name;
  private boolean modificationComments;

  /**
   * 
   */
  public RepositoryConfigModel() {
    // TODO Auto-generated constructor stub
  }

  public String getUrl() {
    return url;
  }
  public void setUrl(String url) {
    String previousVal = this.url;
    this.url = url;
    this.firePropertyChange("url", previousVal, url); //$NON-NLS-1$
    checkIfModelValid();
  }
  public String getName() {
    return name;
  }
  public void setName(String name) {
    String previousVal = this.name;
    this.name = name;
    this.firePropertyChange("name", previousVal, name);   //$NON-NLS-1$
    checkIfModelValid();
  }
  public String getId() {
    return id;
  }
  public void setId(String id) {
    String previousVal = this.id;
    this.id = id;
    this.firePropertyChange("id", previousVal, id);//$NON-NLS-1$
    checkIfModelValid();
  }
  public boolean isModificationComments() {
    return modificationComments;
  }
  public void setModificationComments(boolean modificationComments) {
    boolean previousVal = this.modificationComments;
    this.modificationComments = modificationComments;
    this.firePropertyChange("modificationComments", previousVal, modificationComments);//$NON-NLS-1$
  }
  public void clear() {
    setUrl("");//$NON-NLS-1$
    setId("");//$NON-NLS-1$
    setName("");//$NON-NLS-1$
    setModificationComments(true);
  }
  public void checkIfModelValid() {
    this.firePropertyChange("valid", null, isValid());//$NON-NLS-1$
  }
  public boolean isValid() {
    return url != null && url.length() > 0 && id != null && id.length() > 0 && name != null && name.length() > 0;
  }
}
