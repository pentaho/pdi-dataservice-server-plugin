package org.pentaho.di.ui.repository.repositoryexplorer.model;

import java.util.List;

import org.pentaho.di.repository.IRole;
import org.pentaho.di.repository.IRoleSupportSecurityManager;
import org.pentaho.di.ui.repository.repositoryexplorer.IUIRole;
import org.pentaho.di.ui.repository.repositoryexplorer.UIEEObjectRegistery;

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
      this.changeSupport.firePropertyChange("children", null, this.getChildren());
    }

}
