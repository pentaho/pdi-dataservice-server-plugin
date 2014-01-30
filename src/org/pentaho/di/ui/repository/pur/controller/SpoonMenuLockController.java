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

package org.pentaho.di.ui.repository.pur.controller;

import org.pentaho.di.core.EngineMetaInterface;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.pur.PurRepository;
import org.pentaho.di.repository.pur.model.EEJobMeta;
import org.pentaho.di.repository.pur.model.ILockable;
import org.pentaho.di.repository.pur.model.RepositoryLock;
import org.pentaho.di.ui.repository.pur.services.ILockService;
import org.pentaho.di.ui.spoon.ISpoonMenuController;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.ui.xul.components.XulMenuitem;
import org.pentaho.ui.xul.components.XulToolbarbutton;
import org.pentaho.ui.xul.dom.Document;

public class SpoonMenuLockController implements ISpoonMenuController, java.io.Serializable {

  private static final long serialVersionUID = -1007051375792274091L; /* EESOURCE: UPDATE SERIALVERUID */

  public String getName() {
    return "spoonMenuLockController"; //$NON-NLS-1$
  }

  public void updateMenu(Document doc) {
    try {
      Spoon spoon = Spoon.getInstance();

      // If we are working with an Enterprise Repository
      if ((spoon != null) && (spoon.getRepository() != null) && (spoon.getRepository() instanceof PurRepository)) {
        ILockService service = getService(spoon.getRepository());

        EngineMetaInterface meta = spoon.getActiveMeta();

        // If (meta is not null) and (meta is either a Transformation or Job)
        if ((meta != null) && (meta instanceof ILockable)) {

          RepositoryLock repoLock = null;
          if (service != null && meta.getObjectId() != null) {
            if (meta instanceof EEJobMeta) {
              repoLock = service.getJobLock(meta.getObjectId());
            } else {
              repoLock = service.getTransformationLock(meta.getObjectId());
            }
          }
          // If (there is a lock on this item) and (the UserInfo does not have permission to unlock this file)
          if (repoLock != null) {
            if (!service.canUnlockFileById(meta.getObjectId())) {
              // User does not have modify permissions on this file
              ((XulToolbarbutton) doc.getElementById("toolbar-file-save")).setDisabled(true); //$NON-NLS-1$
              ((XulMenuitem) doc.getElementById("file-save")).setDisabled(true); //$NON-NLS-1$  
            }
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private ILockService getService(Repository repository) throws KettleException {
    if (repository.hasService(ILockService.class)) {
      return (ILockService) repository.getService(ILockService.class);
    } else {
      throw new IllegalStateException();
    }
  }
}
