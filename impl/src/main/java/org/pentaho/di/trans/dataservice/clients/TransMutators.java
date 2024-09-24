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

import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class TransMutators {
  public static void disableAllUnrelatedHops(
    final String stepName, final TransMeta serviceTrans, final boolean includeTargetedSteps ) {
    StepMeta step = serviceTrans.findStep( stepName );

    List<TransHopMeta> allHops =
      IntStream.range( 0, serviceTrans.nrTransHops() )
        .mapToObj( serviceTrans::getTransHop ).collect( toList() );
    HashSet<TransHopMeta> upstreamHops = new HashSet<>();
    findUpstreamHops( upstreamHops, allHops, step, includeTargetedSteps );

    if ( upstreamHops.isEmpty() ) {
      serviceTrans.getSteps().clear();
      serviceTrans.addStep( step );
      IntStream.generate( () -> 0 ).limit( serviceTrans.nrTransHops() ).forEach( serviceTrans::removeTransHop );
    } else {
      allHops.stream().filter( thm -> !upstreamHops.contains( thm ) ).forEach( thm -> thm.setEnabled( false ) );
    }
  }

  private static void findUpstreamHops(
    Set<TransHopMeta> upstreamHops, List<TransHopMeta> all, StepMeta step, final boolean includeTargetedSteps ) {
    for ( TransHopMeta hop : all ) {
      if ( hop.getToStep().equals( step ) && !upstreamHops.contains( hop ) ) {
        upstreamHops.add( hop );
        // the name of chosesTargetSteps makes it a bit unclear what this is doing.  In the case of a Filter step,
        // chosesTargetSteps() returns false.  Part of the Filter definition specifies where it's output goes.  IMO, the
        // method is named backwards, but it's been there forever, so I'll leave it.  Because Filter requires it's target
        // steps to be reachable, we need to leave those hops enabled
        if ( includeTargetedSteps && !hop.getToStep().chosesTargetSteps() ) {
          all.stream().filter( thm -> thm.getFromStep().equals( hop.getToStep() ) )
            .filter( thm -> !upstreamHops.contains( thm ) )
            .forEach( thm -> findUpstreamHops( upstreamHops, all, thm.getToStep(), true ) );
        }
        findUpstreamHops( upstreamHops, all, hop.getFromStep(), includeTargetedSteps );
      }
    }
  }
}
