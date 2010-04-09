package org.pentaho.di.ui.repository.pur.repositoryexplorer.abs;

import org.pentaho.di.ui.repository.capabilities.AbstractRepositoryExplorerUISupport;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.controller.AbsClustersController;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.controller.AbsConnectionsController;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.controller.AbsContextMenuController;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.controller.AbsPartitionsController;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.controller.AbsSlavesController;

public class AbsSecurityProviderUISupport extends AbstractRepositoryExplorerUISupport{

    @Override
    public void setup() {
      AbsConnectionsController absConnectionsController = new AbsConnectionsController();
      AbsPartitionsController absPartitionsController = new AbsPartitionsController();
      AbsSlavesController absSlavesController = new AbsSlavesController();
      AbsClustersController absClustersController = new AbsClustersController();
      AbsContextMenuController absContextMenuController = new AbsContextMenuController(); 
      handlers.add(absConnectionsController);
      controllerNames.add(absConnectionsController.getName());
      handlers.add(absPartitionsController);
      controllerNames.add(absPartitionsController.getName());
      handlers.add(absSlavesController);
      controllerNames.add(absSlavesController.getName());
      handlers.add(absClustersController);
      controllerNames.add(absClustersController.getName());
      handlers.add(absContextMenuController);
      controllerNames.add(absContextMenuController.getName());
    }
  }
