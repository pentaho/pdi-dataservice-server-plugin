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
package org.pentaho.di.trans.dataservice.clients;

import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TransMutatorsTest {
  @BeforeClass
  public static void setUp() throws Exception {
   if( !KettleClientEnvironment.isInitialized() ) {
     KettleClientEnvironment.init();
   }
  }

  @Test
  public void testDisableUnrelatedMultipleDownStream() throws Exception {
    TransMeta transMeta = prepareTrans( "Filter Rows", TransMutators::disableAllUnrelatedHops );
    assertTrue( transMeta.getTransHop( 0 ).isEnabled() );
    assertTrue( transMeta.getTransHop( 1 ).isEnabled() );
    assertFalse( transMeta.getTransHop( 2 ).isEnabled() );
    assertFalse( transMeta.getTransHop( 3 ).isEnabled() );
    assertFalse( transMeta.getTransHop( 4 ).isEnabled() );
  }

  @Test
  public void testDisableUnrelatedSideBranch() throws Exception {
    TransMeta transMeta = prepareTrans( "Write Success", TransMutators::disableAllUnrelatedHops );
    assertTrue( transMeta.getTransHop( 0 ).isEnabled() );
    assertTrue( transMeta.getTransHop( 1 ).isEnabled() );
    assertTrue( transMeta.getTransHop( 2 ).isEnabled() );
    assertFalse( transMeta.getTransHop( 3 ).isEnabled() );
    assertFalse( transMeta.getTransHop( 4 ).isEnabled() );
  }

  @Test
  public void testRemoveDownStreamWalksPassedStepsThatChooseTarget() throws Exception {
    TransMeta transMeta = prepareTrans( "Filter Rows",
      ( stepName, serviceTrans ) -> TransMutators.removeDownstreamSteps( stepName, serviceTrans, true ) );
    assertEquals( 5, transMeta.getTransHopSteps( true ).size() );
    assertTrue( transMeta.getSteps().contains( new StepMeta( "Data Grid", null ) ) );
    assertTrue( transMeta.getSteps().contains( new StepMeta( "Write Unfiltered", null ) ) );
    assertTrue( transMeta.getSteps().contains( new StepMeta( "Filter Rows", null ) ) );
    assertTrue( transMeta.getSteps().contains( new StepMeta( "Write Success", null ) ) );
    assertTrue( transMeta.getSteps().contains( new StepMeta( "Write Failure", null ) ) );
    assertFalse( transMeta.getSteps().contains( new StepMeta( "Write Again", null ) ) );
  }

  @Test
  public void testRemoveDownstreamFromFirstStep() throws Exception {
    TransMeta transMeta = prepareTrans( "Data Grid",
      ( stepName, serviceTrans ) -> TransMutators.removeDownstreamSteps( stepName, serviceTrans, true ) );
    assertEquals( 1, transMeta.getTransHopSteps( true ).size() );
    assertTrue( transMeta.getSteps().contains( new StepMeta( "Data Grid", null ) ) );
    assertFalse( transMeta.getSteps().contains( new StepMeta( "Write Unfiltered", null ) ) );
    assertFalse( transMeta.getSteps().contains( new StepMeta( "Filter Rows", null ) ) );
    assertFalse( transMeta.getSteps().contains( new StepMeta( "Write Success", null ) ) );
    assertFalse( transMeta.getSteps().contains( new StepMeta( "Write Failure", null ) ) );
    assertFalse( transMeta.getSteps().contains( new StepMeta( "Write Again", null ) ) );
  }

  @Test
  public void testRemoveDownStreamSideBranchUntouched() throws Exception {
    TransMeta transMeta = prepareTrans( "Write Success",
      ( stepName, serviceTrans ) -> TransMutators.removeDownstreamSteps( stepName, serviceTrans, true ) );
    assertEquals( 5, transMeta.getTransHopSteps( true ).size() );
    assertTrue( transMeta.getSteps().contains( new StepMeta( "Data Grid", null ) ) );
    assertTrue( transMeta.getSteps().contains( new StepMeta( "Write Unfiltered", null ) ) );
    assertTrue( transMeta.getSteps().contains( new StepMeta( "Filter Rows", null ) ) );
    assertTrue( transMeta.getSteps().contains( new StepMeta( "Write Success", null ) ) );
    assertTrue( transMeta.getSteps().contains( new StepMeta( "Write Failure", null ) ) );
    assertFalse( transMeta.getSteps().contains( new StepMeta( "Write Again", null ) ) );
  }

  @Test
  public void testRemovesAllTheWayDownstream() throws Exception {
    TransMeta transMeta = prepareTrans( "Write Unfiltered",
      ( stepName, serviceTrans ) -> TransMutators.removeDownstreamSteps( stepName, serviceTrans, true ) );
    assertEquals( 2, transMeta.getTransHopSteps( true ).size() );
    assertTrue( transMeta.getSteps().contains( new StepMeta( "Data Grid", null ) ) );
    assertTrue( transMeta.getSteps().contains( new StepMeta( "Write Unfiltered", null ) ) );
    assertFalse( transMeta.getSteps().contains( new StepMeta( "Filter Rows", null ) ) );
    assertFalse( transMeta.getSteps().contains( new StepMeta( "Write Success", null ) ) );
    assertFalse( transMeta.getSteps().contains( new StepMeta( "Write Failure", null ) ) );
    assertFalse( transMeta.getSteps().contains( new StepMeta( "Write Again", null ) ) );
  }
  private TransMeta prepareTrans( String stepname, BiConsumer<String, TransMeta> mutator ) throws KettleException {
    String transPath = this.getClass().getResource( "/TransMutatorsTest.ktr" ).getPath();
    TransMeta transMeta = new TransMeta( transPath );
    TransMeta transSpy = spy( transMeta );
    StepMeta stepSpy = spy( transMeta.findStep( "Filter Rows" ) );
    when( transSpy.findStep( "Filter Rows" ) ).thenReturn( stepSpy );
    when( stepSpy.chosesTargetSteps() ).thenReturn( false );
    mutator.accept( stepname, transSpy );
    return transMeta;
  }
}