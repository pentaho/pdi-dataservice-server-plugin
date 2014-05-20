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

import org.pentaho.di.repository.ObjectRecipient;


public class RepositoryObjectRecipient implements ObjectRecipient, java.io.Serializable {

  private static final long serialVersionUID = 3948870815049027653L; /* EESOURCE: UPDATE SERIALVERUID */

	@Override
  public boolean equals(Object obj) {
	  if(obj != null) {
  	  RepositoryObjectRecipient recipient = (RepositoryObjectRecipient) obj;
  	  if(name == null && type == null && recipient.getName() == null && recipient.getType() == null) {
  	    return true;
  	  } else if(name != null && type != null) {
  	    return name.equals(recipient.getName()) && type.equals(recipient.getType());	    
  	  } else if (recipient.getName() != null && recipient.getType() != null ){
        return recipient.getName().equals(name) && recipient.getType().equals(type);
      } else if (recipient.getType() == null && type == null) {
        return name.equals(recipient.getName());
      } else if (recipient.getName() == null && name == null) {
        return type.equals(recipient.getType());
      } else {
        return false;
      }
	  } else {
	    return false;
	  }
  }

  private String name;

	private Type type;

	// ~ Constructors
	// ====================================================================================================

	public RepositoryObjectRecipient(String name) {
		this(name, Type.USER);
	}

	public RepositoryObjectRecipient(String name, Type type) {
		super();
		this.name = name;
		this.type = type;
	}

	// ~ Methods
	// =========================================================================================================

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

  @Override
  public String toString() {
    return name;
  }
}
