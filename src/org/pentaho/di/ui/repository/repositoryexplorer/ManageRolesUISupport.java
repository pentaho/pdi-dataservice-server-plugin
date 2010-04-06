package org.pentaho.di.ui.repository.repositoryexplorer;

import org.pentaho.di.ui.repository.capabilities.AbstractRepositoryExplorerUISupport;
import org.pentaho.di.ui.repository.repositoryexplorer.controller.EESecurityController;
import org.pentaho.ui.xul.impl.DefaultXulOverlay;

public class ManageRolesUISupport extends AbstractRepositoryExplorerUISupport{

  @Override
  public void setup() {
    EESecurityController manageUserAndRolesController = new EESecurityController();
    handlers.add(manageUserAndRolesController);
    controllerNames.add(manageUserAndRolesController.getName());
    overlays.add(new DefaultXulOverlay("org/pentaho/di/ui/repository/repositoryexplorer/xul/security-with-role-enabled.xul")); //$NON-NLS-1$
  }
}
