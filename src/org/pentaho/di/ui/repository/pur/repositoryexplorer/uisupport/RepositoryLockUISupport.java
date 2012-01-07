/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.ui.repository.pur.repositoryexplorer.uisupport;

import org.pentaho.di.ui.repository.pur.repositoryexplorer.IUIEEUser;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.controller.RepositoryLockController;
import org.pentaho.di.ui.repository.repositoryexplorer.uisupport.AbstractRepositoryExplorerUISupport;
import org.pentaho.di.ui.repository.repositoryexplorer.uisupport.RepositoryExplorerDefaultXulOverlay;

public class RepositoryLockUISupport extends AbstractRepositoryExplorerUISupport implements java.io.Serializable {

  private static final long serialVersionUID = -8629778470133261643L; /* EESOURCE: UPDATE SERIALVERUID */
  @Override
  protected void setup() {
    RepositoryLockController repositoryLockController = new RepositoryLockController();
    
    overlays.add(new RepositoryExplorerDefaultXulOverlay("org/pentaho/di/ui/repository/pur/repositoryexplorer/xul/repository-lock-overlay.xul", IUIEEUser.class)); //$NON-NLS-1$
    controllerNames.add(repositoryLockController.getName());
    handlers.add(repositoryLockController);
  }
}
