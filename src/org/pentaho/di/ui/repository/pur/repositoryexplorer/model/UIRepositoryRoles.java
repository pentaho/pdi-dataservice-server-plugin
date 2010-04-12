package org.pentaho.di.ui.repository.pur.repositoryexplorer.model;

import java.util.List;

import org.pentaho.di.repository.pur.model.IRole;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.IUIRole;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.UIEEObjectRegistery;
import org.pentaho.di.ui.repository.pur.services.IRoleSupportSecurityManager;
import org.pentaho.ui.xul.util.AbstractModelNode;

public class UIRepositoryRoles extends AbstractModelNode<IUIRole> {
  
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
        this.add((IUIRole) UIEEObjectRegistery.getInstance().constructUIRepositoryRole(role));
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
