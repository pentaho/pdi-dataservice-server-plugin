package org.pentaho.di.ui.repository.pur.repositoryexplorer.uisupport;

import org.pentaho.di.ui.repository.capabilities.AbstractRepositoryExplorerUISupport;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.controller.RevisionController;
import org.pentaho.ui.xul.impl.DefaultXulOverlay;

public class RevisionsUISupport extends AbstractRepositoryExplorerUISupport{


  @Override
  protected void setup() {
    RevisionController revisionController = new RevisionController();
    handlers.add(revisionController);
    controllerNames.add(revisionController.getName());
    overlays.add(new DefaultXulOverlay("org/pentaho/di/ui/repository/pur/repositoryexplorer/xul/version-layout-overlay.xul")); //$NON-NLS-1$
  }
}
