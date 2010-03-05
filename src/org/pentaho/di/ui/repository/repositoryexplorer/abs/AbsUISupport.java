package org.pentaho.di.ui.repository.repositoryexplorer.abs;

import org.pentaho.di.ui.repository.capabilities.AbstractRepositoryExplorerUISupport;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.controller.AbsController;
import org.pentaho.ui.xul.impl.DefaultXulOverlay;

public class AbsUISupport extends AbstractRepositoryExplorerUISupport{

  @Override
  public void setup() {
    AbsController absController = new AbsController();
    handlers.add(absController);
    overlays.add(new DefaultXulOverlay("org/pentaho/di/ui/repository/repositoryexplorer/abs/xul/abs-layout-overlay.xul")); //$NON-NLS-1$
  }
}
