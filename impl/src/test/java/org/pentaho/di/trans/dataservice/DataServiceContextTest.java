/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice;

import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.trans.dataservice.streaming.StreamServiceKey;
import org.pentaho.di.trans.dataservice.streaming.execution.StreamingServiceTransExecutor;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */

@RunWith( org.mockito.runners.MockitoJUnitRunner.class )
public class DataServiceContextTest extends BaseTest {
  public static final String EXECUTOR_ID = "Executor ID";
  public static final String STREAMING_EXECUTOR_ID = "Streaming Executor ID";
  public static final String STREAMING_EXECUTOR_ID3 = "Streaming Executor ID X";

  @Mock( answer = Answers.RETURNS_DEEP_STUBS ) DataServiceExecutor dataServiceExecutor;
  @Mock( answer = Answers.RETURNS_DEEP_STUBS ) StreamingServiceTransExecutor streamingExecutor;
  @Mock( answer = Answers.RETURNS_DEEP_STUBS ) StreamingServiceTransExecutor streamingExecutor2;
  @Mock( answer = Answers.RETURNS_DEEP_STUBS ) StreamingServiceTransExecutor streamingExecutor3;

  private StreamServiceKey streamKey;
  private StreamServiceKey streamKey2;
  private StreamServiceKey streamKey3;

  private Map<String, String> mockStreamKeyParams;
  private Map<String, String> mockStreamKeyParams2;

  @Before
  public void setUp() throws Exception {
    context = new DataServiceContext(
      pushDownFactories, autoOptimizationServices,
      cacheManager, uiFactory, logChannel
    );

    mockStreamKeyParams = new HashMap();
    mockStreamKeyParams2 = new HashMap();
    mockStreamKeyParams2.put( "DummyKey", "DummyValue" );

    streamKey = StreamServiceKey.create( STREAMING_EXECUTOR_ID, mockStreamKeyParams );
    streamKey2 = StreamServiceKey.create( STREAMING_EXECUTOR_ID, mockStreamKeyParams2 );
    streamKey3 = StreamServiceKey.create( STREAMING_EXECUTOR_ID3, mockStreamKeyParams );

    when( dataServiceExecutor.getId() ).thenReturn( EXECUTOR_ID );
    when( streamingExecutor.getKey() ).thenReturn( streamKey );
    when( streamingExecutor2.getKey() ).thenReturn( streamKey2 );
    when( streamingExecutor3.getKey() ).thenReturn( streamKey3 );

    assertThat( context.getCacheManager(), sameInstance( cacheManager ) );
    assertThat( context.getUIFactory(), sameInstance( uiFactory ) );
    assertThat( context.getLogChannel(), sameInstance( logChannel ) );
    assertThat( context.getPushDownFactories(), sameInstance( pushDownFactories ) );
    assertThat( context.getAutoOptimizationServices(), sameInstance( autoOptimizationServices ) );
  }

  @Test
  public void testGetMetaStoreUtil() throws Exception {
    assertThat( context.getMetaStoreUtil(), validMetaStoreUtil() );
  }

  @Test
  public void testGetDataServiceDelegate() throws Exception {
    assertThat( context.getDataServiceDelegate(), validMetaStoreUtil() );
  }

  @Test
  public void testExecutor() throws Exception {
    assertNull( context.getExecutor( EXECUTOR_ID ) );
    context.addExecutor( dataServiceExecutor );
    assertThat( context.getExecutor( EXECUTOR_ID ), sameInstance( dataServiceExecutor ) );
    context.removeExecutor( EXECUTOR_ID );
    assertNull( context.getExecutor( EXECUTOR_ID ) );
  }

  @Test
  public void testServiceTransExecutor() throws Exception {
    assertNull( context.getServiceTransExecutor( streamKey ) );
    context.addServiceTransExecutor( streamingExecutor );
    assertThat( context.getServiceTransExecutor( streamKey ), sameInstance( streamingExecutor ) );
    context.removeServiceTransExecutor( streamKey );
    assertNull( context.getServiceTransExecutor( streamKey ) );
    verify( streamingExecutor ).stopAll();
  }

  @Test
  public void testRemoveServiceTransExecutor() throws Exception {
    assertNull( context.getServiceTransExecutor( streamKey ) );
    assertNull( context.getServiceTransExecutor( streamKey2 ) );
    assertNull( context.getServiceTransExecutor( streamKey3 ) );
    context.addServiceTransExecutor( streamingExecutor );
    context.addServiceTransExecutor( streamingExecutor2 );
    context.addServiceTransExecutor( streamingExecutor3 );

    assertThat( context.getServiceTransExecutor( streamKey ), sameInstance( streamingExecutor ) );
    assertThat( context.getServiceTransExecutor( streamKey2 ), sameInstance( streamingExecutor2 ) );
    assertThat( context.getServiceTransExecutor( streamKey3 ), sameInstance( streamingExecutor3 ) );

    context.removeServiceTransExecutor( STREAMING_EXECUTOR_ID );
    assertNull( context.getServiceTransExecutor( streamKey ) );
    assertNull( context.getServiceTransExecutor( streamKey2 ) );
    assertThat( context.getServiceTransExecutor( streamKey3 ), sameInstance( streamingExecutor3 ) );

    context.removeServiceTransExecutor( STREAMING_EXECUTOR_ID3 );
    assertNull( context.getServiceTransExecutor( streamKey ) );
    assertNull( context.getServiceTransExecutor( streamKey2 ) );
    assertNull( context.getServiceTransExecutor( streamKey3 ) );
  }

  @Test
  public void testRemoveInexistingServiceTransExecutor() throws Exception {
    assertNull( context.getServiceTransExecutor( streamKey ) );
    context.removeServiceTransExecutor( STREAMING_EXECUTOR_ID );
    verify( streamingExecutor, times( 0 ) ).stopAll();
  }

  protected Matcher<DataServiceMetaStoreUtil> validMetaStoreUtil() {
    return allOf(
      hasProperty( "context", sameInstance( context ) ),
      hasProperty( "stepCache", sameInstance( cache ) ),
      hasProperty( "logChannel", sameInstance( logChannel ) ),
      hasProperty( "pushDownFactories", sameInstance( pushDownFactories ) )
    );
  }
}
