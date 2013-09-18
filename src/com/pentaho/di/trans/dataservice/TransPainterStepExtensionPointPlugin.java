null
null
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
