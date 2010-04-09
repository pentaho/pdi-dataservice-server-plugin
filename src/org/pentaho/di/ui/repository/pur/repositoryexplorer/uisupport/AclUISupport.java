package org.pentaho.di.ui.repository.pur.repositoryexplorer.uisupport;

import org.pentaho.di.ui.repository.capabilities.AbstractRepositoryExplorerUISupport;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.controller.PermissionsController;
import org.pentaho.ui.xul.impl.DefaultXulOverlay;

public class AclUISupport extends AbstractRepositoryExplorerUISupport {

  @Override
  protected void setup() {
    overlays.add(new DefaultXulOverlay(
    "org/pentaho/di/ui/repository/repositoryexplorer/xul/acl-layout-overlay.xul")); //$NON-NLS-1$
    PermissionsController permissionsController = new PermissionsController();
    controllerNames.add(permissionsController.getName());
    handlers.add(permissionsController);
  }

}
