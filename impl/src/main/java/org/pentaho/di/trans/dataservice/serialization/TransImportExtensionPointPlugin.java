/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.di.trans.dataservice.serialization;

import java.util.function.Function;
import jakarta.annotation.Nullable;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;

@ExtensionPoint(
    id = "TransImportExtensionPointPlugin",
    extensionPointId = "TransImportAfterSaveToRepo",
    description = "Synchronize data services references in metastore after transformation is imported to repository"
  )
public class TransImportExtensionPointPlugin implements ExtensionPointInterface {
  private Class<TransImportExtensionPointPlugin> PKG = TransImportExtensionPointPlugin.class;
  private DataServiceReferenceSynchronizer referenceSynchronizer;

  public TransImportExtensionPointPlugin( DataServiceReferenceSynchronizer referenceSynchronizer ) {
    this.referenceSynchronizer = referenceSynchronizer;
  }

  @Override public void callExtensionPoint( LogChannelInterface log, Object o ) throws KettleException {
    if ( !( o instanceof TransMeta ) ) {
      return;
    }

    TransMeta transMeta = (TransMeta) o;
    referenceSynchronizer.sync( transMeta, getExceptionHandler( log ), true );
  }

  Function<Exception, Void> getExceptionHandler( final LogChannelInterface log ) {
    return new Function<Exception, Void>() {
      @Nullable @Override public Void apply( @Nullable Exception e ) {
        if ( e instanceof DataServiceAlreadyExistsException ) {
          DataServiceAlreadyExistsException dsaee = (DataServiceAlreadyExistsException) e;
          log.logBasic( BaseMessages.getString( PKG, "Messages.Import.Overwrite",
              dsaee.getDataServiceMeta().getName() ) );
        } else {
          log.logError( BaseMessages.getString( PKG, "Messages.Import.Error" ), e );
        }
        return null;
      }
    };
  }
}
