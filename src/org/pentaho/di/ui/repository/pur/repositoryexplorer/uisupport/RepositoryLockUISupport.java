package org.pentaho.di.ui.repository.pur.repositoryexplorer.uisupport;

import org.pentaho.di.ui.repository.pur.repositoryexplorer.IUIEEUser;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.controller.RepositoryLockController;
import org.pentaho.di.ui.repository.repositoryexplorer.uisupport.AbstractRepositoryExplorerUISupport;
import org.pentaho.di.ui.repository.repositoryexplorer.uisupport.RepositoryExplorerDefaultXulOverlay;

public class RepositoryLockUISupport extends AbstractRepositoryExplorerUISupport{
  @Override
  protected void setup() {
    RepositoryLockController repositoryLockController = new RepositoryLockController();
    
    overlays.add(new RepositoryExplorerDefaultXulOverlay("org/pentaho/di/ui/repository/pur/repositoryexplorer/xul/repository-lock-overlay.xul", IUIEEUser.class)); //$NON-NLS-1$
    controllerNames.add(repositoryLockController.getName());
    handlers.add(repositoryLockController);
  }
}
