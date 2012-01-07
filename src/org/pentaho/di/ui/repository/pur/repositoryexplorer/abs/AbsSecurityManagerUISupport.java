/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.ui.repository.pur.repositoryexplorer.abs;

import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.controller.AbsController;
import org.pentaho.di.ui.repository.repositoryexplorer.uisupport.AbstractRepositoryExplorerUISupport;
import org.pentaho.di.ui.repository.repositoryexplorer.uisupport.RepositoryExplorerDefaultXulOverlay;

public class AbsSecurityManagerUISupport extends AbstractRepositoryExplorerUISupport implements java.io.Serializable {

  private static final long serialVersionUID = -3161444723795411324L; /* EESOURCE: UPDATE SERIALVERUID */

  @Override
  public void setup() {
    AbsController absController = new AbsController();
    handlers.add(absController);
    controllerNames.add(absController.getName());
    overlays.add(new RepositoryExplorerDefaultXulOverlay("org/pentaho/di/ui/repository/pur/repositoryexplorer/abs/xul/abs-layout-overlay.xul", AbsSecurityManagerUISupport.class)); //$NON-NLS-1$
  }
}
