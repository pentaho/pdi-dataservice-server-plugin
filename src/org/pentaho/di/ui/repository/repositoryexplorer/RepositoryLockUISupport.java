package org.pentaho.di.ui.repository.repositoryexplorer;

import org.pentaho.di.ui.repository.capabilities.AbstractRepositoryExplorerUISupport;
import org.pentaho.di.ui.repository.pur.services.RepositoryLockController;
import org.pentaho.ui.xul.impl.DefaultXulOverlay;

public class RepositoryLockUISupport extends AbstractRepositoryExplorerUISupport{
  @Override
  protected void setup() {
    RepositoryLockController repositoryLockController = new RepositoryLockController();
    
    overlays.add(new DefaultXulOverlay("org/pentaho/di/ui/repository/pur/xul/repository-lock-overlay.xul")); //$NON-NLS-1$
    controllerNames.add(repositoryLockController.getName());
    handlers.add(repositoryLockController);
  }
}
