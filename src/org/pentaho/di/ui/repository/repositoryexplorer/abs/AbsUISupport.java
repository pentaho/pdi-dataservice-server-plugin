package org.pentaho.di.ui.repository.repositoryexplorer.abs;

import org.pentaho.di.ui.repository.capabilities.AbstractRepositoryExplorerUISupport;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.controller.AbsClustersController;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.controller.AbsConnectionsController;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.controller.AbsController;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.controller.AbsPartitionsController;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.controller.AbsSlavesController;
import org.pentaho.ui.xul.impl.DefaultXulOverlay;

public class AbsUISupport extends AbstractRepositoryExplorerUISupport{

  @Override
  public void setup() {
    AbsController absController = new AbsController();
    handlers.add(absController);
    controllerNames.add(absController.getName());
    AbsConnectionsController absConnectionsController = new AbsConnectionsController();
    AbsPartitionsController absPartitionsController = new AbsPartitionsController();
    AbsSlavesController absSlavesController = new AbsSlavesController();
    AbsClustersController absClustersController = new AbsClustersController();
    handlers.add(absConnectionsController);
    controllerNames.add(absConnectionsController.getName());
    handlers.add(absPartitionsController);
    controllerNames.add(absPartitionsController.getName());
    handlers.add(absSlavesController);
    controllerNames.add(absSlavesController.getName());
    handlers.add(absClustersController);
    controllerNames.add(absClustersController.getName());
    overlays.add(new DefaultXulOverlay("org/pentaho/di/ui/repository/repositoryexplorer/abs/xul/abs-layout-overlay.xul")); //$NON-NLS-1$
  }
}
