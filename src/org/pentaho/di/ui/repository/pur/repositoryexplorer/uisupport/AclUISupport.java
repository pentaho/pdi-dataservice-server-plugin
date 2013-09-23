/*!
* The Pentaho proprietary code is licensed under the terms and conditions
* of the software license agreement entered into between the entity licensing
* such code and Pentaho Corporation.
*
* This software costs money - it is not free
*
* Copyright 2002 - 2013 Pentaho Corporation.  All rights reserved.
*/

package org.pentaho.di.ui.repository.pur.repositoryexplorer.uisupport;

import org.pentaho.di.ui.repository.pur.repositoryexplorer.IUIEEUser;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.controller.PermissionsController;
import org.pentaho.di.ui.repository.repositoryexplorer.uisupport.AbstractRepositoryExplorerUISupport;
import org.pentaho.di.ui.repository.repositoryexplorer.uisupport.RepositoryExplorerDefaultXulOverlay;

public class AclUISupport extends AbstractRepositoryExplorerUISupport implements java.io.Serializable {

  private static final long serialVersionUID = 3699454343113328922L; /* EESOURCE: UPDATE SERIALVERUID */

  @Override
  protected void setup() {
    overlays.add(new RepositoryExplorerDefaultXulOverlay(
    "org/pentaho/di/ui/repository/pur/repositoryexplorer/xul/acl-layout-overlay.xul", IUIEEUser.class)); //$NON-NLS-1$
    PermissionsController permissionsController = new PermissionsController();
    controllerNames.add(permissionsController.getName());
    handlers.add(permissionsController);
  }

}
