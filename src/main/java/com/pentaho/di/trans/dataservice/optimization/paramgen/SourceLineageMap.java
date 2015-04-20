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
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.pentaho.di.trans.dataservice.DataServiceMeta;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import com.pentaho.metaverse.api.ChangeType;
import com.pentaho.metaverse.api.StepFieldOperations;
import com.pentaho.metaverse.api.model.Operations;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import java.text.MessageFormat;
import java.util.Iterator;
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

  @Override protected SetMultimap<String, List<StepFieldOperations>> delegate() {
    return storage;
  }

  public SourceLineageMap filterChangedValues() {
    for ( Iterator<List<StepFieldOperations>> iterator = values().iterator(); iterator.hasNext(); ) {
      if ( Iterables.any( iterator.next(), new Predicate<StepFieldOperations>() {
        @Override public boolean apply( StepFieldOperations stepFieldOperations ) {
          Operations operations = stepFieldOperations.getOperations();
          return operations != null && operations.containsKey( ChangeType.DATA );
        }
      } ) ) {
        iterator.remove();
      }
    }
    return this;
  }

  public SourceLineageMap filterSupportedInputs( TransMeta transMeta,
                                                   ParameterGenerationFactory serviceProvider ) {
    for ( Iterator<String> iterator = keySet().iterator(); iterator.hasNext(); ) {
      String inputStep = iterator.next();
      StepMeta stepMeta = transMeta.findStep( inputStep );
      if ( stepMeta == null || !serviceProvider.supportsStep( stepMeta ) ) {
        iterator.remove();
      }
    }
    return this;
  }

  public SourceLineageMap filterExistingOptimizations( DataServiceMeta dataServiceMeta ) {
    for ( PushDownOptimizationMeta existing : dataServiceMeta.getPushDownOptimizationMeta() ) {
      if ( existing.getType() instanceof ParameterGeneration ) {
        keySet().remove( existing.getStepName() );
      }
    }
    return this;
  }

  public List<PushDownOptimizationMeta> generateOptimizationList() {
    Map<String, Set<List<StepFieldOperations>>> inputSteps = Multimaps.asMap( this );
    List<PushDownOptimizationMeta> optimizationList = Lists.newArrayListWithExpectedSize( inputSteps.size() );
    for ( Map.Entry<String, Set<List<StepFieldOperations>>> inputStepLineage : inputSteps.entrySet() ) {
      String inputStep = inputStepLineage.getKey();
      Set<List<StepFieldOperations>> lineageSet = inputStepLineage.getValue();
      PushDownOptimizationMeta pushDownOptimizationMeta = new PushDownOptimizationMeta();
      ParameterGeneration parameterGeneration = new ParameterGeneration();
      pushDownOptimizationMeta.setName( MessageFormat.format( "Parameter Generator: {0}", inputStep ) );
      pushDownOptimizationMeta.setStepName( inputStep );
      pushDownOptimizationMeta.setType( parameterGeneration );
      parameterGeneration.setParameterName( "DATA_SERVICE_QUERY_" + inputStep.replaceAll( "\\s", "_" ).toUpperCase() );
      for ( List<StepFieldOperations> fieldLineage : lineageSet ) {
        StepFieldOperations origin = fieldLineage.get( 0 );
        StepFieldOperations last = Iterables.getLast( fieldLineage );
        parameterGeneration.createFieldMapping( origin.getFieldName(), last.getFieldName() );
      }
      optimizationList.add( pushDownOptimizationMeta );
    }
    return optimizationList;
  }
}
