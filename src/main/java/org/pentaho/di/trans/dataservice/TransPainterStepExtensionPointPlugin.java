/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
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
import org.pentaho.metastore.api.exceptions.MetaStoreException;

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

    try {
      DataServiceMeta dataService = metaStoreUtil.getDataServiceByStepName( transMeta, stepMeta.getName() );
      if ( dataService != null ) {
        // Is this step a data service provider?
        //
        extension.gc
          .drawImage( "images/data-services.svg", getClass().getClassLoader(), extension.x1 - 11,
            extension.y1 - 9 + extension.iconsize );
        extension.areaOwners.add(
          new AreaOwner( AreaType.CUSTOM, extension.x1 - 11, extension.y1 - 9 + extension.iconsize, 16, 16,
            extension.offset, transMeta, stepMeta ) );
      }
    } catch ( MetaStoreException e ) {
      // Don't draw anything in the event of an error
    }

  }

}
