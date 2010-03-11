package org.pentaho.di.ui.repository.repositoryexplorer.trash;

import org.pentaho.di.ui.repository.capabilities.AbstractRepositoryExplorerUISupport;
import org.pentaho.ui.xul.impl.DefaultXulOverlay;

public class TrashUISupport extends AbstractRepositoryExplorerUISupport {

  @Override
  protected void setup() {
    TrashBrowseController deleteController = new TrashBrowseController();
    handlers.add(deleteController);
    overlays.add(new DefaultXulOverlay("org/pentaho/di/ui/repository/repositoryexplorer/trash/trash-overlay.xul")); //$NON-NLS-1$
  }

}
