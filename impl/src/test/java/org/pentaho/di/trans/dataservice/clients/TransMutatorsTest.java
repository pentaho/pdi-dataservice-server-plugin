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
package org.pentaho.di.trans.dataservice.clients;

import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;

import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransMutatorsTest {
  @BeforeClass
  public static void setUp() throws Exception {
    if ( !KettleClientEnvironment.isInitialized() ) {
      KettleClientEnvironment.init();
    }
  }

  @Test
  public void testDisableAllButFirst() throws Exception {
    TransMeta transMeta = prepareTrans( "Tasty Dishes",
      ( stepName, serviceTrans ) -> TransMutators.disableAllUnrelatedHops( stepName, serviceTrans, true ) );
    assertEquals( 1, transMeta.nrSteps() );
    assertEquals( 0, transMeta.nrTransHops() );
  }

  @Test
  public void testDisablesAllTheWayDownStream() throws Exception {
    TransMeta transMeta = prepareTrans( "Write Unfiltered",
      ( stepName, serviceTrans ) -> TransMutators.disableAllUnrelatedHops( stepName, serviceTrans, true ) );
    assertHopsEnabled( transMeta, false, false, false, false, false, false );
  }

  @Test
  public void testDisableKeepsRequiredDownstreamSteps() throws Exception {
    TransMeta transMeta = prepareTrans( "Filter Rows",
      ( stepName, serviceTrans ) -> TransMutators.disableAllUnrelatedHops( stepName, serviceTrans, true ) );
    assertHopsEnabled( transMeta, true, true, true, false, true,  true );
  }

  @Test
  public void testDisableKeepsIndirectRequiredDownstreamSteps() throws Exception {
    TransMeta transMeta = prepareTrans( "Write Success",
      ( stepName, serviceTrans ) -> TransMutators.disableAllUnrelatedHops( stepName, serviceTrans, true ) );
    assertHopsEnabled( transMeta, true, true, true, false, true, true );
  }

  @Test
  public void testDisableKeepsCycledStep() throws Exception {
    TransMeta transMeta = prepareTrans( "Map Failures",
      ( stepName, serviceTrans ) -> TransMutators.disableAllUnrelatedHops( stepName, serviceTrans, true ) );
    assertHopsEnabled( transMeta, true, true, true, false, true, true );
  }

  @Test
  public void testDisableFromFinalStep() throws Exception {
    TransMeta transMeta = prepareTrans( "Write Again",
      ( stepName, serviceTrans ) -> TransMutators.disableAllUnrelatedHops( stepName, serviceTrans, true ) );
    assertHopsEnabled( transMeta, true, true, true, true, true, true );
  }

  @Test
  public void testDisableWithoutIncludingTargets() throws Exception {
    TransMeta transMeta = prepareTrans( "Filter Rows",
      ( stepName, serviceTrans ) -> TransMutators.disableAllUnrelatedHops( stepName, serviceTrans, false ) );
    assertHopsEnabled( transMeta, true, false, false, false, true, false );
  }

  private TransMeta prepareTrans( String stepname, BiConsumer<String, TransMeta> mutator ) throws KettleException {
    String transPath = this.getClass().getResource( "/TransMutatorsTest.ktr" ).getPath();
    TransMeta transMeta = new TransMeta( transPath );
    StreamInterface mockStream = mock( StreamInterface.class );
    when( mockStream.getStreamType() ).thenReturn( StreamInterface.StreamType.TARGET );
    transMeta.findStep( "Filter Rows" ).getStepMetaInterface().getStepIOMeta().addStream( mockStream );
    mutator.accept( stepname, transMeta );
    return transMeta;
  }

  private void assertHopsEnabled(
    final TransMeta transMeta,
    boolean unfilteredFilter,
    boolean filterSuccess,
    boolean filterFailure,
    boolean successAgain,
    boolean grossFilter,
    boolean failureSuccess ) {
    assertEquals( "Hop from Tasty to Unfiltered incorrect", true, transMeta.getTransHop( 0 ).isEnabled() );
    assertEquals( "Hop from Unfiltered to Filter incorrect", unfilteredFilter, transMeta.getTransHop( 1 ).isEnabled() );
    assertEquals( "Hop from Filter to Success incorrect", filterSuccess, transMeta.getTransHop( 2 ).isEnabled() );
    assertEquals( "Hop from Filter to Failure incorrect", filterFailure, transMeta.getTransHop( 3 ).isEnabled() );
    assertEquals( "Hop from Success to Again incorrect", successAgain, transMeta.getTransHop( 4 ).isEnabled() );
    assertEquals( "Hop from Gross to Filter incorrect", grossFilter, transMeta.getTransHop( 5 ).isEnabled() );
    assertEquals( "Hop from Failure to Success incorrect", failureSuccess, transMeta.getTransHop( 6 ).isEnabled() );
  }
}
