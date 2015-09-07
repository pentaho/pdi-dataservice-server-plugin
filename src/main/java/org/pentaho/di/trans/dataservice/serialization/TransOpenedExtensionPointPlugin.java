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

package org.pentaho.di.trans.dataservice.serialization;

import com.google.common.base.Functions;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.listeners.ContentChangedAdapter;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;

/**
 * @author nhudak
 */
@ExtensionPoint(
  id = "TransOpenedExtensionPointPlugin",
  extensionPointId = "TransAfterOpen",
  description = "Registers a ContentChangedListener on each TransMeta to synchronize Data Services with the MetaStore"
)
public class TransOpenedExtensionPointPlugin implements ExtensionPointInterface {
  private final DataServiceContext context;

  public TransOpenedExtensionPointPlugin( DataServiceContext context ) {
    this.context = context;
  }

  @Override public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    if ( object instanceof TransMeta ) {
      final TransMeta transMeta = (TransMeta) object;
      transMeta.addContentChangedListener( new ContentChangedAdapter() {
        @Override public void contentSafe( Object parentObject ) {
          try {
            context.getMetaStoreUtil().sync( transMeta, Functions.constant( null ) );
          } catch ( Exception e ) {
            context.getLogChannel().logError( "Unable to sync repository", e );
          }
        }
      } );
    }
  }
}
