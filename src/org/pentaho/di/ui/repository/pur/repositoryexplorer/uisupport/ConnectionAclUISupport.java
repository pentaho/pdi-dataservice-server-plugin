/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.ui.repository.pur.repositoryexplorer.uisupport;

import java.io.Serializable;

import org.pentaho.di.ui.repository.pur.repositoryexplorer.IUIEEUser;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.controller.ConnectionPermissionsController;
import org.pentaho.di.ui.repository.repositoryexplorer.uisupport.AbstractRepositoryExplorerUISupport;
import org.pentaho.di.ui.repository.repositoryexplorer.uisupport.RepositoryExplorerDefaultXulOverlay;

/**
 * This support class registered the new ConnectionPermissionsController and xul overlay for managing
 * Database Connection Permissions in the UI.
 * 
 * @author Will Gorman (wgorman@pentaho.com)
 *
 */
public class ConnectionAclUISupport extends AbstractRepositoryExplorerUISupport implements Serializable {

  private static final long serialVersionUID = -1381250312418398039L; /* EESOURCE: UPDATE SERIALVERUID */

  @Override
  protected void setup() {
    overlays.add(new RepositoryExplorerDefaultXulOverlay(
    "org/pentaho/di/ui/repository/pur/repositoryexplorer/xul/connection-acl-overlay.xul", IUIEEUser.class)); //$NON-NLS-1$
    ConnectionPermissionsController connAclController = new ConnectionPermissionsController();
    controllerNames.add(connAclController.getName());
    handlers.add(connAclController);
  }

}
