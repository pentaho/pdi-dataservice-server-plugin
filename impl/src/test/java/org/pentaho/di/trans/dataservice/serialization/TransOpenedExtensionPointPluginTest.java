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
