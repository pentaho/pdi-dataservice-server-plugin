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

import java.util.HashSet;
import java.util.Set;

import org.pentaho.di.repository.IUser;

public class EERoleInfo implements IRole, java.io.Serializable {

  private static final long serialVersionUID = -7422069585209086417L; /* EESOURCE: UPDATE SERIALVERUID */

	public static final String REPOSITORY_ELEMENT_TYPE = "role"; //$NON-NLS-1$

	// ~ Instance fields
	// =================================================================================================

	private String name;

	private String description;

	private Set<IUser> users;

	// ~ Constructors
	// ====================================================================================================

	public EERoleInfo() {
		this.name = null;
		this.description = null;
    users = new HashSet<IUser>();
	}

	public void setName(String name) {
		this.name = name;
	}

	public EERoleInfo(String name) {
		this(name, null);
	}

	public EERoleInfo(String name, String description) {
	  this();
		this.name = name;
		this.description = description;
	}
  public EERoleInfo(String name, String description, Set<IUser> users) {
    this(name, description);
    this.users = users;
  }

	// ~ Methods
	// =========================================================================================================

	public String getName() {
		return name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public void setUsers(Set<IUser> users) {
		this.users = users;
	}

	public Set<IUser> getUsers() {
		return users;
	}

	public boolean addUser(IUser user) {
		return users.add(user);
	}

	public boolean removeUser(IUser user) {
		return users.remove(user);
	}

	public void clearUsers() {
		users.clear();
	}

  public IRole getRole() {
    // TODO Auto-generated method stub
    return this;
  }
}
