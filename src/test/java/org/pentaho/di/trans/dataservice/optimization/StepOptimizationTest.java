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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.BaseTest;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import org.pentaho.di.trans.step.StepMetaInterface;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */

@RunWith( org.mockito.runners.MockitoJUnitRunner.class )
public class StepOptimizationTest extends BaseTest {
  private static final String OPTIMIZED_STEP = "OPTIMIZED STEP";
  @Mock( answer = Answers.CALLS_REAL_METHODS ) StepOptimization stepOptimization;
  @Mock DataServiceExecutor executor;
  @Mock Trans serviceTrans;
  @Mock StepInterface stepInterface;
  PushDownOptimizationMeta meta;

  @Before
  public void setUp() throws Exception {
    meta = new PushDownOptimizationMeta();
    meta.setStepName( OPTIMIZED_STEP );

    when( executor.getServiceTrans() ).thenReturn( serviceTrans );
    when( serviceTrans.findRunThread( OPTIMIZED_STEP ) ).thenReturn( stepInterface );
  }

  @Test
  public void testActivate() throws Exception {
    final ArrayListMultimap<DataServiceExecutor.ExecutionPoint, Runnable> tasks = ArrayListMultimap.create();
    doReturn( true ).when( stepOptimization ).activate( executor, stepInterface );
    when( executor.getListenerMap() ).thenReturn( tasks );

    final ListenableFuture<Boolean> activate = stepOptimization.activate( executor, meta );

    assertThat( tasks.get( DataServiceExecutor.ExecutionPoint.OPTIMIZE ), hasSize( 1 ) );
    assertThat( activate.isDone(), is( false ) );
    verify( serviceTrans, never() ).findRunThread( anyString() );

    tasks.get( DataServiceExecutor.ExecutionPoint.OPTIMIZE ).get( 0 ).run();

    verify( stepOptimization ).activate( executor, stepInterface );
    assertThat( activate.isDone(), is( true ) );
    assertThat( activate.get(), is( true ) );
  }

  @Test
  public void testPreview() throws Exception {
    OptimizationImpactInfo impactInfo = new OptimizationImpactInfo( OPTIMIZED_STEP );
    doReturn( impactInfo ).when( stepOptimization ).preview( executor, stepInterface );
    StepMetaDataCombi stepStruct = new StepMetaDataCombi() {
      {
        stepMeta = mock( StepMeta.class );
        stepname = OPTIMIZED_STEP;
        copy = 0;

        step = stepInterface;
        meta = mock( StepMetaInterface.class );
        data = mock( StepDataInterface.class );
      }
    };
    when( serviceTrans.getSteps() ).thenReturn( ImmutableList.of( stepStruct ) );

    assertThat( stepOptimization.preview( executor, meta ), sameInstance( impactInfo ) );

    final InOrder inOrder = inOrder( serviceTrans, stepInterface, stepOptimization );

    // Verify trans is prepared
    inOrder.verify( serviceTrans ).prepareExecution( null );
    inOrder.verify( serviceTrans ).findRunThread( OPTIMIZED_STEP );

    // Run preview
    inOrder.verify( stepOptimization ).preview( executor, stepInterface );

    // Clean up
    inOrder.verify( stepInterface ).setOutputDone();
    inOrder.verify( stepInterface ).dispose( stepStruct.meta, stepStruct.data );
    inOrder.verify( stepInterface ).markStop();
  }

}
