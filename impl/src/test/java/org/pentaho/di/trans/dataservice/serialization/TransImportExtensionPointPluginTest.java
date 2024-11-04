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

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransImportExtensionPointPluginTest {
  @Test
  public void callExtensionPoint() throws Exception {
    DataServiceReferenceSynchronizer referenceSynchronizer = mock( DataServiceReferenceSynchronizer.class );

    TransMeta transMeta = mock( TransMeta.class );
    Repository transRepository = mock( Repository.class );
    IMetaStore transMetaStore = mock( IMetaStore.class );
    when( transRepository.getRepositoryMetaStore() ).thenReturn( transMetaStore );
    when( transMeta.getRepository() ).thenReturn( transRepository );
    when( transMeta.getMetaStore() ).thenReturn( transMetaStore );

    TransImportExtensionPointPlugin plugin = new TransImportExtensionPointPlugin( referenceSynchronizer );
    LogChannelInterface log = mock( LogChannelInterface.class );

    plugin.callExtensionPoint( log, null );
    verify( referenceSynchronizer, times( 0 ) ).sync( same( transMeta ), any( Function.class ), eq( true ) );
    plugin.callExtensionPoint( log, "Not TransMeta" );
    verify( referenceSynchronizer, times( 0 ) ).sync( same( transMeta ), any( Function.class ), eq( true ) );

    plugin.callExtensionPoint( log, transMeta );

    ArgumentCaptor<Function> exceptionHandlerCaptor = ArgumentCaptor.forClass( Function.class );

    verify( referenceSynchronizer ).sync( same( transMeta ), exceptionHandlerCaptor.capture(), eq( true ) );

    Exception e = new Exception();
    exceptionHandlerCaptor.getValue().apply( e );
    verify( log ).logError( anyString(), same( e ) );
    DataServiceMeta dsMeta = mock( DataServiceMeta.class );
    DataServiceAlreadyExistsException dsaee = new DataServiceAlreadyExistsException( dsMeta );
    exceptionHandlerCaptor.getValue().apply( dsaee );
    verify( log ).logBasic( anyString() );
  }
}
