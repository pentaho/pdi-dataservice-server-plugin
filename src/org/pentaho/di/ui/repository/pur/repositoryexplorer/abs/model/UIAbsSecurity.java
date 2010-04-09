package org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.model;

import org.pentaho.di.repository.RepositorySecurityManager;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.IUIRole;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.IUIAbsRole;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.model.UIEESecurity;

public class UIAbsSecurity extends UIEESecurity {

  public UIAbsSecurity() {
    super();
  }

  public UIAbsSecurity(RepositorySecurityManager rsm) throws Exception {
    super(rsm);
  }

  public void addLogicalRole(String logicalRole) {
    IUIRole role = getSelectedRole();
    if (role instanceof IUIAbsRole) {
      ((IUIAbsRole) role).addLogicalRole(logicalRole);
    } else {
      throw new IllegalStateException();
    }
  }

  public void removeLogicalRole(String logicalRole) {
    IUIRole role = getSelectedRole();
    if (role instanceof IUIAbsRole) {
      ((IUIAbsRole) role).removeLogicalRole(logicalRole);
    } else {
      throw new IllegalStateException();
    }
  }

}
