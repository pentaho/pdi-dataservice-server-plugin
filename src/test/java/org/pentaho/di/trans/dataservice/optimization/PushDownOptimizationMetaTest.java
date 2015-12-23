/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.optimization;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PushDownOptimizationMetaTest {

  private PushDownOptimizationMeta pushDownOptimizationMeta;
  private DataServiceExecutor executor;
  private PushDownType pushDownType;
  public static final String STEP_NAME = "Optimized Step";

  @Before
  public void setUp() throws Exception {
    pushDownOptimizationMeta = new PushDownOptimizationMeta();
    executor = mock( DataServiceExecutor.class );
    pushDownType = mock( PushDownType.class );

    pushDownOptimizationMeta.setStepName( STEP_NAME );
    pushDownOptimizationMeta.setType( pushDownType );
  }

  @Test
  public void testActivate() throws Exception {
    ListenableFuture<Boolean> future = Futures.immediateFuture( true );
    when( pushDownType.activate( executor, pushDownOptimizationMeta ) ).thenReturn( future );
    assertThat( pushDownOptimizationMeta.activate( executor ), sameInstance( future ) );
  }

  @Test
  public void testPreview() throws Exception {
    OptimizationImpactInfo info = new OptimizationImpactInfo( STEP_NAME );
    info.setModified( true );
    when( pushDownType.preview( executor, pushDownOptimizationMeta ) ).thenReturn( info );

    assertThat( pushDownOptimizationMeta.preview( executor ), sameInstance( info ) );

    pushDownOptimizationMeta.setEnabled( false );
    OptimizationImpactInfo disabled = pushDownOptimizationMeta.preview( executor );
    assertThat( disabled.isModified(), is( false ) );
    assertThat( disabled.getDescription(), startsWith( "#Optimization is disabled" ) );
  }
}
