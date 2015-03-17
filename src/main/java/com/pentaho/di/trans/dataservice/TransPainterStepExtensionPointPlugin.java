/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2015 Pentaho Corporation (Pentaho). All rights reserved.
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

package com.pentaho.di.trans.dataservice;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.gui.AreaOwner;
import org.pentaho.di.core.gui.AreaOwner.AreaType;
import org.pentaho.di.core.gui.PrimitiveGCInterface.EImage;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPainterExtension;
import org.pentaho.di.trans.step.StepMeta;

@ExtensionPoint(
    id="TransPainterStepExtensionPointPlugin",
    extensionPointId="TransPainterStep",
    description="Paint a database icon over a step in case a data service is defined"
  )
public class TransPainterStepExtensionPointPlugin implements ExtensionPointInterface {
  // private static Class<?> PKG = TransPainterStepExtensionPointPlugin.class; // for i18n purposes, needed by Translator2!!

  @Override
  public void callExtensionPoint(LogChannelInterface log, Object object) throws KettleException {
    if (!(object instanceof TransPainterExtension)) return;
    TransPainterExtension extension = (TransPainterExtension) object;
    TransMeta transMeta= extension.transMeta;
    StepMeta stepMeta = extension.stepMeta;
    
    String dataServiceName = transMeta.getAttribute(DataServiceMetaStoreUtil.GROUP_DATA_SERVICE, DataServiceMetaStoreUtil.DATA_SERVICE_NAME);
    String dataServiceStep = transMeta.getAttribute(DataServiceMetaStoreUtil.GROUP_DATA_SERVICE, DataServiceMetaStoreUtil.DATA_SERVICE_STEPNAME);
    if (!Const.isEmpty(dataServiceName) && !Const.isEmpty(dataServiceStep)) {
      // Is this step a data service provider?
      //
      if (stepMeta.getName().equalsIgnoreCase(dataServiceStep)) {
        // Draw a database icon in the upper right corner.
        //
        extension.gc.drawImage(EImage.DB, extension.x1-8+extension.iconsize, extension.y1-8);
        extension.areaOwners.add(new AreaOwner(AreaType.CUSTOM, extension.x1-8+extension.iconsize, extension.y1-8, 16, 16, extension.offset, transMeta, stepMeta));
      }
    }
  
  }

}
