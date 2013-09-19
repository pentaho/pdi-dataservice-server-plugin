/*!
* The Pentaho proprietary code is licensed under the terms and conditions
* of the software license agreement entered into between the entity licensing
* such code and Pentaho Corporation.
*
* This software costs money - it is not free
*
* Copyright 2002 - 2013 Pentaho Corporation.  All rights reserved.
*/

package org.pentaho.di.ui.repository.pur.repositoryexplorer.model;

import java.util.List;

import org.pentaho.di.repository.pur.model.IRole;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.IUIRole;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.UIEEObjectRegistery;
import org.pentaho.di.ui.repository.pur.services.IRoleSupportSecurityManager;
import org.pentaho.ui.xul.util.AbstractModelNode;

public class UIRepositoryRoles extends AbstractModelNode<IUIRole> implements java.io.Serializable {

  private static final long serialVersionUID = -3386655857939500874L; /* EESOURCE: UPDATE SERIALVERUID */
  
    public UIRepositoryRoles(){
    }
    
    public UIRepositoryRoles(List<IUIRole> roles){
      super(roles);
    }

    public UIRepositoryRoles(IRoleSupportSecurityManager rsm) {

      List<IRole> roleList; 
      try {
        roleList = rsm.getRoles();
        for (IRole role : roleList) {
        this.add(UIEEObjectRegistery.getInstance().constructUIRepositoryRole(role));
        }
      } catch (Exception e) {
        // TODO: handle exception; can't get users???
      }
    }
    
    @Override
    protected void fireCollectionChanged() {
      this.changeSupport.firePropertyChange("children", null, this);
    }

}
