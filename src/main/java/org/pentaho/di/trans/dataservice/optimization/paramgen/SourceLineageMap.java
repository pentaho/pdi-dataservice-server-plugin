/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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
