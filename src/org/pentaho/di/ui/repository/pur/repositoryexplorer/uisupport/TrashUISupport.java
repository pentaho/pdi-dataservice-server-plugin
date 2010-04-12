package org.pentaho.di.ui.repository.pur.repositoryexplorer.uisupport;

import org.pentaho.di.ui.repository.pur.repositoryexplorer.IUIEEUser;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.controller.TrashBrowseController;
import org.pentaho.di.ui.repository.repositoryexplorer.uisupport.AbstractRepositoryExplorerUISupport;
import org.pentaho.di.ui.repository.repositoryexplorer.uisupport.RepositoryExplorerDefaultXulOverlay;

public class TrashUISupport extends AbstractRepositoryExplorerUISupport {

  @Override
  protected void setup() {
    TrashBrowseController deleteController = new TrashBrowseController();
    handlers.add(deleteController);
    overlays.add(new RepositoryExplorerDefaultXulOverlay("org/pentaho/di/ui/repository/pur/repositoryexplorer/xul/trash-overlay.xul", IUIEEUser.class)); //$NON-NLS-1$
  }

}
