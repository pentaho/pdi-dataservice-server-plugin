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

package org.pentaho.di.trans.dataservice.optimization.paramgen;

import com.google.common.base.Predicate;
import com.google.common.collect.ForwardingSetMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import org.pentaho.metaverse.api.StepFieldOperations;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SourceLineageMap extends ForwardingSetMultimap<String, List<StepFieldOperations>> {
  private SetMultimap<String, List<StepFieldOperations>> storage;

  protected SourceLineageMap( SetMultimap<String, List<StepFieldOperations>> storage ) {
    this.storage = storage;
  }

  public static SourceLineageMap create() {
    return new SourceLineageMap( HashMultimap.<String, List<StepFieldOperations>>create() );
  }

  public static SourceLineageMap create( Map<String, Set<List<StepFieldOperations>>> operationPaths ) {
    SourceLineageMap sourceLineageMap = create();
    for ( List<StepFieldOperations> lineage : Iterables.concat( operationPaths.values() ) ) {
      if ( ( lineage != null ) && !lineage.isEmpty() ) {
        String inputStep = lineage.get( 0 ).getStepName();
        sourceLineageMap.put( inputStep, lineage );
      }
    }
    return sourceLineageMap;
  }

  @Override protected SetMultimap<String, List<StepFieldOperations>> delegate() {
    return storage;
  }

  public SourceLineageMap filter( Predicate<? super Map.Entry<String, List<StepFieldOperations>>> predicate ) {
    return new SourceLineageMap( Multimaps.filterEntries( storage, predicate ) );
  }

  public SourceLineageMap filterKeys( Predicate<String> predicate ) {
    return new SourceLineageMap( Multimaps.filterKeys( storage, predicate ) );
  }

}
