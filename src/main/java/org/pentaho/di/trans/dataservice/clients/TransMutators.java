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

import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class TransMutators {
  public static void disableAllUnrelatedHops( final String stepName, final TransMeta serviceTrans ) {
    StepMeta step = serviceTrans.findStep( stepName );
    List<TransHopMeta> transHops = new ArrayList<>( serviceTrans.nrTransHops() );

    for ( int i = 0; i < serviceTrans.nrTransHops(); i++ ) {
      transHops.add( serviceTrans.getTransHop( i ) );
    }
    HashSet<TransHopMeta> hops = new HashSet<>();
    findUpstreamHops( hops, transHops, step );

    for ( TransHopMeta transHopMeta : transHops ) {
      if ( !hops.contains( transHopMeta ) ) {
        transHopMeta.setEnabled( false );
      }
    }
  }

  private static void findUpstreamHops( Set<TransHopMeta> hops, List<TransHopMeta> all, StepMeta step ) {
    for ( TransHopMeta hop : all ) {
      if ( hop.getToStep().equals( step ) && !hops.contains( hop ) ) {
        hops.add( hop );
        findUpstreamHops( hops, all, hop.getFromStep() );
      }
    }
  }

  public static void removeDownstreamSteps( final String stepName, final TransMeta serviceTrans, final boolean top ) {
    StepMeta step = serviceTrans.findStep( stepName );
    List<TransHopMeta> allTransHopFrom = serviceTrans.findAllTransHopFrom( step );

    // the name of chosesTargetSteps makes it a bit unclear what it's doing.  In the case of a Filter step,
    // chosesTargetSteps() returns false.  Part of the Filter definition specifies where it's output goes.  IMO, the
    // method is named backwards, but it's been there forever, so I'll leave it.  Because Filter requires it's target
    // steps to be reachable, we need to leave those hops enabled but still disable ones further downstream
    final Consumer<TransHopMeta> recurseConsumer =
      thm -> removeDownstreamSteps( thm.getToStep().getName(), serviceTrans, false );
    final Consumer<TransHopMeta> transHopMetaConsumer = !top || step.chosesTargetSteps()
      ? recurseConsumer.andThen( thm -> serviceTrans.removeStep( serviceTrans.getSteps().indexOf( thm.getToStep() ) ) )
                       .andThen( serviceTrans::removeTransHop )
      : recurseConsumer;

    for ( TransHopMeta transHopMeta : allTransHopFrom ) {
      transHopMetaConsumer.accept( transHopMeta );
    }
  }
}
