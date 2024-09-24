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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class TransOpenedExtensionPointPluginTest {

  @Mock SynchronizationListener service;
  @Mock TransMeta transMeta;

  TransOpenedExtensionPointPlugin extensionPointPlugin;

  @Before
  public void setUp() throws Exception {
    DataServiceContext context = mock( DataServiceContext.class );
    DataServiceDelegate delegate = mock( DataServiceDelegate.class );

    when( context.getDataServiceDelegate() ).thenReturn( delegate );
    when( delegate.createSyncService() ).thenReturn( service );

    extensionPointPlugin = new TransOpenedExtensionPointPlugin( context );
  }

  @Test
  public void testCallExtensionPoint() throws Exception {
    extensionPointPlugin.callExtensionPoint( mock( LogChannel.class ), transMeta );

    verify( service ).install( transMeta );
  }
}
