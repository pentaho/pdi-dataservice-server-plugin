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

import java.util.HashSet;
import java.util.Set;

import org.pentaho.di.repository.IUser;
import org.pentaho.di.repository.pur.model.IRole;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.IUIRole;
import org.pentaho.di.ui.repository.repositoryexplorer.model.IUIUser;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIObjectCreationException;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIObjectRegistry;
import org.pentaho.ui.xul.XulEventSourceAdapter;

public class UIRepositoryRole extends XulEventSourceAdapter implements IUIRole, java.io.Serializable {

  private static final long serialVersionUID = 6397782451758458788L; /* EESOURCE: UPDATE SERIALVERUID */

	private IRole rri;

	public UIRepositoryRole() {
	}

	public UIRepositoryRole(IRole rri) {
		this.rri = rri;
	}

	public String getName() {
		return rri.getName();
	}

	public void setName(String name) {
		rri.setName(name);
	}
	
	public String getDescription() {
		return rri.getDescription();
	}

	public void setDescription(String description) {
		rri.setDescription(description);
	}

	public void setUsers(Set<IUIUser> users) {
	  Set<IUser> rusers = new HashSet<IUser>();
	  for(IUIUser user:users) {
	    rusers.add(user.getUserInfo());
	  }
		rri.setUsers(rusers);
	}

	public Set<IUIUser> getUsers() {
	  Set<IUIUser> rusers = new HashSet<IUIUser>();
	  for(IUser userInfo:rri.getUsers()) {
	    try {
        rusers.add(UIObjectRegistry.getInstance().constructUIRepositoryUser(userInfo));
      } catch (UIObjectCreationException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
	  }
		return rusers;
	}

	public boolean addUser(IUIUser user) {
		return rri.addUser(user.getUserInfo());
	}

	public boolean removeUser(IUIUser user) {
		return removeUser(user.getUserInfo().getLogin());
	}

	public void clearUsers() {
		rri.clearUsers();
	}

	public IRole getRole() {
	  return rri;
	}

	private boolean removeUser(String userName) {
	  IUser userInfo = null;
	  for(IUser user:rri.getUsers()) {
	    if(user.getLogin().equals(userName)) {
	      userInfo = user;
	      break;
	    }
	  }
	  if(userInfo != null) {
	    return rri.removeUser(userInfo);
	  } else {
	    return false;
	  }
	}
}
