package org.pentaho.di.ui.repository.pur.repositoryexplorer.uisupport;

import org.pentaho.di.ui.repository.pur.repositoryexplorer.IUIEEUser;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.controller.RevisionController;
import org.pentaho.di.ui.repository.repositoryexplorer.uisupport.AbstractRepositoryExplorerUISupport;
import org.pentaho.di.ui.repository.repositoryexplorer.uisupport.RepositoryExplorerDefaultXulOverlay;

public class RevisionsUISupport extends AbstractRepositoryExplorerUISupport{


  @Override
  protected void setup() {
    RevisionController revisionController = new RevisionController();
    handlers.add(revisionController);
    controllerNames.add(revisionController.getName());
    overlays.add(new RepositoryExplorerDefaultXulOverlay("org/pentaho/di/ui/repository/pur/repositoryexplorer/xul/version-layout-overlay.xul", IUIEEUser.class)); //$NON-NLS-1$
  }
}
