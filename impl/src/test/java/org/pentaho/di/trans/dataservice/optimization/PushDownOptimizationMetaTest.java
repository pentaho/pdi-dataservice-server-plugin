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
