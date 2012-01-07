/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.ui.repository.pur.repositoryexplorer.uisupport;

import org.pentaho.di.ui.repository.pur.repositoryexplorer.IUIEEUser;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.controller.TrashBrowseController;
import org.pentaho.di.ui.repository.repositoryexplorer.uisupport.AbstractRepositoryExplorerUISupport;
import org.pentaho.di.ui.repository.repositoryexplorer.uisupport.RepositoryExplorerDefaultXulOverlay;

public class TrashUISupport extends AbstractRepositoryExplorerUISupport implements java.io.Serializable {

  private static final long serialVersionUID = -2216932367290742613L; /* EESOURCE: UPDATE SERIALVERUID */

  @Override
  protected void setup() {
    TrashBrowseController deleteController = new TrashBrowseController();
    handlers.add(deleteController);
    overlays.add(new RepositoryExplorerDefaultXulOverlay("org/pentaho/di/ui/repository/pur/repositoryexplorer/xul/trash-overlay.xul", IUIEEUser.class)); //$NON-NLS-1$
  }

}
