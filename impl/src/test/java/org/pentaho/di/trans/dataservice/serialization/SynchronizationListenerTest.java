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

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.listeners.ContentChangedListener;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class SynchronizationListenerTest {

  @Mock TransMeta transMeta;
  @Mock DataServiceDelegate delegate;
  @Mock LogChannel logChannel;
  @Mock DataServiceMeta dataServiceMeta;
  @Mock DataServiceReferenceSynchronizer synchronizer;
  @InjectMocks SynchronizationListener service;

  @Captor ArgumentCaptor<Function<? super Exception, ?>> errorHandler;

  @Test
  public void testInstall() throws Exception {
    when( transMeta.getContentChangedListeners() ).thenReturn( ImmutableList.<ContentChangedListener>of() );

    service.install( transMeta );

    verify( transMeta ).addStepChangeListener( service );
    verify( transMeta ).addContentChangedListener( service );

    lenient().when( transMeta.getContentChangedListeners() ).thenReturn( ImmutableList.<ContentChangedListener>of( service ) );

    verifyNoMoreInteractions( ignoreStubs( transMeta ) );
  }

  @Before
  public void setUp() throws Exception {
    when( delegate.getLogChannel() ).thenReturn( logChannel );
    doAnswer( new Answer() {
      @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
        ( (Runnable) invocation.getArguments()[0] ).run();
        return null;
      }
    } ).when( delegate ).syncExec( any( Runnable.class ) );
  }

  @Test
  public void testContentSafe() throws Exception {
    service.contentSafe( transMeta );

    verify( synchronizer ).sync( same( transMeta ), errorHandler.capture() );

    UndefinedDataServiceException undefinedException = new UndefinedDataServiceException( dataServiceMeta );
    when( delegate.showPrompt( anyString(), anyString() ) ).thenReturn( false, true );
    errorHandler.getValue().apply( undefinedException );
    verify( delegate, never() ).removeDataService( dataServiceMeta, false );
    errorHandler.getValue().apply( undefinedException );
    verify( delegate ).removeDataService( dataServiceMeta );
    verify( logChannel, times( 2 ) ).logError( anyString(), same( undefinedException ) );

    DataServiceAlreadyExistsException conflictException = new DataServiceAlreadyExistsException( dataServiceMeta );
    errorHandler.getValue().apply( conflictException );
    verify( delegate ).suggestEdit( same( dataServiceMeta ), anyString(), anyString() );
    verify( logChannel ).logError( anyString(), same( conflictException ) );

    MetaStoreException metaStoreException = new MetaStoreException();
    errorHandler.getValue().apply( metaStoreException );
    verify( logChannel ).logError( any(), same( metaStoreException ) );
  }

  @Test
  public void testNoPrompt() throws Exception {
    service.contentSafe( transMeta );
    service.setPrompt( false );

    verify( synchronizer ).sync( same( transMeta ), errorHandler.capture() );

    verify( delegate, never() ).showPrompt( anyString(), anyString() );
    verify( delegate, never() ).removeDataService( dataServiceMeta );

    verify( delegate, never() ).suggestEdit( same( dataServiceMeta ), anyString(), anyString() );

    MetaStoreException metaStoreException = new MetaStoreException();
    errorHandler.getValue().apply( metaStoreException );
    verify( logChannel ).logError( any(), same( metaStoreException ) );
  }

  @Test
  public void testOnStepChange() throws Exception {
    StepMeta oldMeta = mock( StepMeta.class );
    StepMeta newMeta = mock( StepMeta.class );

    when( oldMeta.getName() ).thenReturn( "ORIGINAL STEP" );
    when( newMeta.getName() ).thenReturn( "ORIGINAL STEP" );
    service.onStepChange( transMeta, oldMeta, newMeta );

    when( newMeta.getName() ).thenReturn( "DIFFERENT STEP" );
    service.onStepChange( transMeta, oldMeta, newMeta );

    verifyNoMoreInteractions( dataServiceMeta );

    when( delegate.getDataServiceByStepName( transMeta, "ORIGINAL STEP" ) )
      .thenReturn( dataServiceMeta );

    service.onStepChange( transMeta, oldMeta, newMeta );
    verify( dataServiceMeta ).setStepname( "DIFFERENT STEP" );
    verify( delegate ).save( dataServiceMeta );
  }
}
