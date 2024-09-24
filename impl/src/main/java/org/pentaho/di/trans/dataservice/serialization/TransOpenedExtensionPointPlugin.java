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
