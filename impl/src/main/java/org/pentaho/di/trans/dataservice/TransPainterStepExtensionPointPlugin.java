/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.dataservice;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.gui.AreaOwner;
import org.pentaho.di.core.gui.AreaOwner.AreaType;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPainterExtension;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.trans.step.StepMeta;

@ExtensionPoint(
  id = "TransPainterStepExtensionPointPlugin",
  extensionPointId = "TransPainterStep",
  description = "Paint a database icon over a step in case a data service is defined"
  )
public class TransPainterStepExtensionPointPlugin implements ExtensionPointInterface {
  // private static Class<?> PKG = TransPainterStepExtensionPointPlugin.class; // for i18n purposes, needed by
  // Translator2!!

  private DataServiceMetaStoreUtil metaStoreUtil;

  public TransPainterStepExtensionPointPlugin( DataServiceContext context ) {
    this.metaStoreUtil = context.getMetaStoreUtil();
  }

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    if ( !( object instanceof TransPainterExtension ) ) {
      return;
    }
    TransPainterExtension extension = (TransPainterExtension) object;
    TransMeta transMeta = extension.transMeta;
    StepMeta stepMeta = extension.stepMeta;

    DataServiceMeta dataService = metaStoreUtil.getDataServiceByStepName( transMeta, stepMeta.getName() );
    if ( dataService != null && dataService.isUserDefined() ) {
      // Is this step a data service provider?
      //

      String image = dataService.isStreaming()
          ? "images/data-services-streaming.svg"
          : "images/data-services.svg";

      extension.gc
          .drawImage( image, getClass().getClassLoader(), extension.x1 - 13,
              extension.y1 - 8 + extension.iconsize );

      extension.areaOwners.add(
        new AreaOwner( AreaType.CUSTOM, extension.x1 - 13, extension.y1 - 8 + extension.iconsize, 16, 16,
          extension.offset, transMeta, stepMeta ) );
    }

  }

}
