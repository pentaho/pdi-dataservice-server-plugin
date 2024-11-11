/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.dataservice;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.gui.GCInterface;
import org.pentaho.di.core.gui.Point;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPainterExtension;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.trans.step.StepMeta;

import java.util.ArrayList;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bmorrise on 10/26/15.
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class TransPainterStepExtensionPointPluginTest {

  private static String STEP_NAME = "Step Name";

  @Mock
  private DataServiceMetaStoreUtil metaStoreUtil;

  @Mock
  private DataServiceContext context;

  @Mock
  private TransPainterExtension extension;

  @Mock
  private TransMeta transMeta;

  @Mock
  private StepMeta stepMeta;

  @Mock
  private DataServiceMeta dataServiceMeta;

  @Mock
  private LogChannelInterface log;

  @Mock
  private GCInterface gc;

  private TransPainterStepExtensionPointPlugin plugin;

  @Before
  public void setUp() throws Exception {
    when( context.getMetaStoreUtil() ).thenReturn( metaStoreUtil );
    plugin = new TransPainterStepExtensionPointPlugin( context );
  }

  @Test
  public void testCallExtensionPoint() throws Exception {
    extension.transMeta = transMeta;
    extension.stepMeta = stepMeta;
    extension.gc = gc;
    extension.x1 = 0;
    extension.y1 = 0;
    extension.iconsize = 32;
    extension.offset = new Point( 0, 0 );
    extension.areaOwners = new ArrayList<>();
    when( stepMeta.getName() ).thenReturn( STEP_NAME );
    when( metaStoreUtil.getDataServiceByStepName( transMeta, STEP_NAME ) ).thenReturn( dataServiceMeta );
    when( dataServiceMeta.isUserDefined() ).thenReturn( true );

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass( String.class );
    when( dataServiceMeta.isStreaming() ).thenReturn( false );

    plugin.callExtensionPoint( log, extension );

    verify( metaStoreUtil ).getDataServiceByStepName( transMeta, STEP_NAME );
    verify( gc ).drawImage( captor.capture(), any( ClassLoader.class ), anyInt(), anyInt() );
    assert( captor.getValue().equals( "images/data-services.svg" ) );
  }

  @Test
  public void testCallExtensionPointWithStreamingDataService() throws Exception {
    extension.transMeta = transMeta;
    extension.stepMeta = stepMeta;
    extension.gc = gc;
    extension.x1 = 0;
    extension.y1 = 0;
    extension.iconsize = 32;
    extension.offset = new Point( 0, 0 );
    extension.areaOwners = new ArrayList<>();
    when( stepMeta.getName() ).thenReturn( STEP_NAME );
    when( metaStoreUtil.getDataServiceByStepName( transMeta, STEP_NAME ) ).thenReturn( dataServiceMeta );
    when( dataServiceMeta.isUserDefined() ).thenReturn( true );

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass( String.class );
    when( dataServiceMeta.isStreaming() ).thenReturn( true );

    plugin.callExtensionPoint( log, extension );

    verify( metaStoreUtil ).getDataServiceByStepName( transMeta, STEP_NAME );
    verify( gc ).drawImage( captor.capture(), any( ClassLoader.class ), anyInt(), anyInt() );
    assert( captor.getValue().equals( "images/data-services-streaming.svg" ) );
  }

  @Test
  public void testCallExtensionPointTransient() throws Exception {
    extension.transMeta = transMeta;
    extension.stepMeta = stepMeta;
    extension.gc = gc;
    extension.x1 = 0;
    extension.y1 = 0;
    extension.iconsize = 32;
    extension.offset = new Point( 0, 0 );
    extension.areaOwners = new ArrayList<>();
    when( stepMeta.getName() ).thenReturn( STEP_NAME );
    when( metaStoreUtil.getDataServiceByStepName( transMeta, STEP_NAME ) ).thenReturn( dataServiceMeta );
    when( dataServiceMeta.isUserDefined() ).thenReturn( false );

    plugin.callExtensionPoint( log, extension );

    verify( metaStoreUtil ).getDataServiceByStepName( transMeta, STEP_NAME );
    verify( gc, never() ).drawImage( anyString(), any( ClassLoader.class ), anyInt(), anyInt() );
  }

}
