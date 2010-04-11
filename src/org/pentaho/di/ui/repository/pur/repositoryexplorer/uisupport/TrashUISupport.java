package org.pentaho.di.ui.repository.pur.repositoryexplorer.uisupport;

import org.pentaho.di.ui.repository.capabilities.AbstractRepositoryExplorerUISupport;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.controller.TrashBrowseController;
import org.pentaho.ui.xul.impl.DefaultXulOverlay;

public class TrashUISupport extends AbstractRepositoryExplorerUISupport {

  @Override
  protected void setup() {
    TrashBrowseController deleteController = new TrashBrowseController();
    handlers.add(deleteController);
    overlays.add(new DefaultXulOverlay("org/pentaho/di/ui/repository/pur/repositoryexplorer/xul/trash-overlay.xul")); //$NON-NLS-1$
  }

}
