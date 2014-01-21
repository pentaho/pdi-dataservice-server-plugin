/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package org.pentaho.di.ui.repository.pur.repositoryexplorer.abs;

import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.controller.AbsClustersController;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.controller.AbsConnectionsController;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.controller.AbsContextMenuController;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.controller.AbsPartitionsController;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.controller.AbsSlavesController;
import org.pentaho.di.ui.repository.repositoryexplorer.uisupport.AbstractRepositoryExplorerUISupport;

public class AbsSecurityProviderUISupport extends AbstractRepositoryExplorerUISupport implements java.io.Serializable {

  private static final long serialVersionUID = -5965263581796252745L; /* EESOURCE: UPDATE SERIALVERUID */

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
