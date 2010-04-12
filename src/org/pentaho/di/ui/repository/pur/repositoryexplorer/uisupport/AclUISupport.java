package org.pentaho.di.ui.repository.pur.repositoryexplorer.uisupport;

import org.pentaho.di.ui.repository.pur.repositoryexplorer.IUIEEUser;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.controller.PermissionsController;
import org.pentaho.di.ui.repository.repositoryexplorer.uisupport.AbstractRepositoryExplorerUISupport;
import org.pentaho.di.ui.repository.repositoryexplorer.uisupport.RepositoryExplorerDefaultXulOverlay;

public class AclUISupport extends AbstractRepositoryExplorerUISupport {

  @Override
  protected void setup() {
    overlays.add(new RepositoryExplorerDefaultXulOverlay(
    "org/pentaho/di/ui/repository/pur/repositoryexplorer/xul/acl-layout-overlay.xul", IUIEEUser.class)); //$NON-NLS-1$
    PermissionsController permissionsController = new PermissionsController();
    controllerNames.add(permissionsController.getName());
    handlers.add(permissionsController);
  }

}
