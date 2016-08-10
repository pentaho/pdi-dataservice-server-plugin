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
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.metastore.api.IMetaStore;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransImportExtensionPointPluginTest {
  @Test
  public void callExtensionPoint() throws Exception {
    DataServiceMetaStoreUtil metaStoreUtil = mock( DataServiceMetaStoreUtil.class );
    DataServiceContext context = mock( DataServiceContext.class );
    when( context.getMetaStoreUtil() ).thenReturn( metaStoreUtil );

    TransMeta transMeta = mock( TransMeta.class );
    Repository transRepository = mock( Repository.class );
    IMetaStore transMetaStore = mock( IMetaStore.class );
    when( transRepository.getMetaStore() ).thenReturn( transMetaStore );
    when( transMeta.getRepository() ).thenReturn( transRepository );
    when( transMeta.getMetaStore() ).thenReturn( transMetaStore );

    TransImportExtensionPointPlugin plugin = new TransImportExtensionPointPlugin( context );
    LogChannelInterface log = mock( LogChannelInterface.class );

    plugin.callExtensionPoint( log, null );
    verify( metaStoreUtil, times( 0 ) ).sync( same( transMeta ), any( Function.class ) );
    plugin.callExtensionPoint( log, "Not TransMeta" );
    verify( metaStoreUtil, times( 0 ) ).sync( same( transMeta ), any( Function.class ) );

    plugin.callExtensionPoint( log, transMeta );

    ArgumentCaptor<Function> exceptionHandlerCaptor = ArgumentCaptor.forClass( Function.class );

    verify( metaStoreUtil ).sync( same( transMeta ), exceptionHandlerCaptor.capture() );

    Exception e = new Exception();
    exceptionHandlerCaptor.getValue().apply( e );
    verify( log ).logError( anyString(), same( e ) );
  }
}
