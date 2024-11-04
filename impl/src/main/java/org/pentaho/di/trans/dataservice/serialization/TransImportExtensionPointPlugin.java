/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.di.trans.dataservice.serialization;

import java.util.function.Function;
import javax.annotation.Nullable;
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
