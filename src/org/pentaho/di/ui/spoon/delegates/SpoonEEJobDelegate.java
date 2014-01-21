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

package org.pentaho.di.ui.spoon.delegates;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.repository.pur.services.ILockService;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.TabMapEntry;
import org.pentaho.xul.swt.tab.TabItem;

public class SpoonEEJobDelegate extends SpoonJobDelegate implements java.io.Serializable {

  private static final long serialVersionUID = 5658845199854709546L; /* EESOURCE: UPDATE SERIALVERUID */
  ILockService service;
  public SpoonEEJobDelegate(Spoon spoon) {
    super(spoon);
    Repository repository = spoon.getRepository();
    try {
      if(repository.hasService(ILockService.class)) {
        service = (ILockService) repository.getService(ILockService.class);
      }  else {
        throw new IllegalStateException();
      }
    } catch (KettleException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void addJobGraph(JobMeta jobMeta) {
    super.addJobGraph(jobMeta);
    TabMapEntry tabEntry = spoon.delegates.tabs.findTabMapEntry(jobMeta);
    if(tabEntry != null) {
      TabItem tabItem = tabEntry.getTabItem();
      try {
        if((service != null) && (jobMeta.getObjectId() != null) && (service.getJobLock(jobMeta.getObjectId()) != null)) {
          tabItem.setImage(GUIResource.getInstance().getImageLocked());
        }
      } catch(Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

}
