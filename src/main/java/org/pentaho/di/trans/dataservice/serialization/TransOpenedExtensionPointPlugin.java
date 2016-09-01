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

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;

/**
 * @author nhudak
 */
@ExtensionPoint(
  id = "TransOpenedExtensionPointPlugin",
  extensionPointId = "TransAfterOpen",
  description = "Registers a ContentChangedListener and StepMetaChangeListener on each TransMeta to synchronize Data Services with the MetaStore"
  )
public class TransOpenedExtensionPointPlugin implements ExtensionPointInterface {
  private final SynchronizationListener synchronizationListener;

  public TransOpenedExtensionPointPlugin( DataServiceContext context ) {
    this.synchronizationListener = context.getDataServiceDelegate().createSyncService();
  }

  @Override public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    final TransMeta transMeta = (TransMeta) object;
    synchronizationListener.install( transMeta );
  }
}
