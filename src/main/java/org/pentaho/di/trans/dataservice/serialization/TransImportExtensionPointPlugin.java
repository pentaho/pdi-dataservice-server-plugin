/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

import com.google.common.base.Function;
import javax.annotation.Nullable;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;

@ExtensionPoint(
    id = "TransImportExtensionPointPlugin",
    extensionPointId = "TransImportAfterSaveToRepo",
    description = "Synchronize data services references in metastore after transformation is imported to repository"
  )
public class TransImportExtensionPointPlugin implements ExtensionPointInterface {
  private Class<TransImportExtensionPointPlugin> PKG = TransImportExtensionPointPlugin.class;
  private DataServiceMetaStoreUtil metaStoreUtil;

  public TransImportExtensionPointPlugin( DataServiceContext context ) {
    metaStoreUtil = context.getMetaStoreUtil();
  }

  @Override public void callExtensionPoint( LogChannelInterface log, Object o ) throws KettleException {
    if ( !( o instanceof TransMeta ) ) {
      return;
    }

    TransMeta transMeta = (TransMeta) o;
    metaStoreUtil.sync( transMeta, getExceptionHandler( log ) );
  }

  Function<Exception, Void> getExceptionHandler( final LogChannelInterface log ) {
    return new Function<Exception, Void>() {
      @Nullable @Override public Void apply( @Nullable Exception e ) {
        log.logError( BaseMessages.getString( PKG, "Messages.ImportError" ), e );
        return null;
      }
    };
  }
}
