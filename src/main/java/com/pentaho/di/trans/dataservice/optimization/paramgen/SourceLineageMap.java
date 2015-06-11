/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2015 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package com.pentaho.di.trans.dataservice.optimization.paramgen;

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
