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
import org.pentaho.di.trans.dataservice.streaming.execution.StreamingServiceTransExecutor;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */

@RunWith( org.mockito.runners.MockitoJUnitRunner.class )
public class DataServiceContextTest extends BaseTest {
  public static final String EXECUTOR_ID = "Executor ID";
  public static final String STREAMING_EXECUTOR_ID = "Streaming Executor ID";
  @Mock( answer = Answers.RETURNS_DEEP_STUBS ) DataServiceExecutor dataServiceExecutor;
  @Mock( answer = Answers.RETURNS_DEEP_STUBS ) StreamingServiceTransExecutor streamingExecutor;

  @Before
  public void setUp() throws Exception {
    context = new DataServiceContext(
      pushDownFactories, autoOptimizationServices,
      cacheManager, uiFactory, logChannel
    );

    when( dataServiceExecutor.getId() ).thenReturn( EXECUTOR_ID );
    when( streamingExecutor.getId() ).thenReturn( STREAMING_EXECUTOR_ID );

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
    assertNull( context.getServiceTransExecutor( STREAMING_EXECUTOR_ID ) );
    context.addServiceTransExecutor( streamingExecutor );
    assertThat( context.getServiceTransExecutor( STREAMING_EXECUTOR_ID ), sameInstance( streamingExecutor ) );
    context.removeServiceTransExecutor( STREAMING_EXECUTOR_ID );
    assertNull( context.getServiceTransExecutor( STREAMING_EXECUTOR_ID ) );
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
