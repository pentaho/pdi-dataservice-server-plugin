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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.pentaho.di.trans.dataservice.DataServiceMeta;
import com.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import com.pentaho.di.trans.dataservice.optimization.PushDownType;
import org.pentaho.metaverse.api.ChangeType;
import org.pentaho.metaverse.api.ILineageClient;
import org.pentaho.metaverse.api.StepFieldOperations;
import org.pentaho.metaverse.api.model.Operations;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;

/**
 * @author nhudak
 */
public class AutoParameterGenerationService implements AutoOptimizationService {
  final private ILineageClient lineageClient;
  final private ParameterGenerationFactory serviceProvider;

  public AutoParameterGenerationService( ILineageClient lineageClient,
                                         ParameterGenerationFactory serviceProvider ) {
    this.lineageClient = lineageClient;
    this.serviceProvider = serviceProvider;
  }

  @Override public List<PushDownOptimizationMeta> apply( TransMeta transMeta, DataServiceMeta dataServiceMeta ) {
    LogChannelInterface logChannel = transMeta.getLogChannel() != null ? transMeta.getLogChannel() : LogChannel.GENERAL;
    try {
      String stepName = dataServiceMeta.getStepname();
      List<String> fields = Arrays.asList( transMeta.getStepFields( stepName ).getFieldNames() );
      Map<String, Set<List<StepFieldOperations>>> operationPaths = lineageClient.getOperationPaths( transMeta, stepName, fields );
      SourceLineageMap sourceLineageMap = SourceLineageMap.create( operationPaths )
        .filterKeys( stepSupported( transMeta ) )
        .filterKeys( not( in( parametrizedSteps( dataServiceMeta ) ) ) )
        .filter( fieldUnchanged() );

      return generateOptimizationList( sourceLineageMap );
    } catch ( Throwable e ) {
      logChannel.logError( "Unable to run Auto-Optimization", e );
      return Collections.emptyList();
    }
  }

  protected Set<String> parametrizedSteps( DataServiceMeta dataServiceMeta ) {
    HashSet<String> parametrizedSteps = Sets.newHashSet();
    for ( PushDownOptimizationMeta existing : dataServiceMeta.getPushDownOptimizationMeta() ) {
      if ( existing.getType() instanceof ParameterGeneration ) {
        parametrizedSteps.add( existing.getStepName() );
      }
    }
    return parametrizedSteps;
  }

  private Predicate<Map.Entry<String, List<StepFieldOperations>>> fieldUnchanged() {
    return new Predicate<Map.Entry<String, List<StepFieldOperations>>>() {
      @Override public boolean apply( Map.Entry<String, List<StepFieldOperations>> entry ) {
        for ( StepFieldOperations stepFieldOperations : entry.getValue() ) {
          Operations operations = stepFieldOperations.getOperations();
          if ( operations != null && operations.containsKey( ChangeType.DATA ) ) {
            return false;
          }
        }
        return true;
      }
    };
  }

  private Predicate<String> stepSupported( final TransMeta transMeta ) {
    return new Predicate<String>() {
      @Override public boolean apply( String inputStep ) {
        StepMeta stepMeta = transMeta.findStep( inputStep );
        return stepMeta != null && serviceProvider.supportsStep( stepMeta );
      }
    };
  }

  private List<PushDownOptimizationMeta> generateOptimizationList( SourceLineageMap sourceLineageMap ) {
    Map<String, Set<List<StepFieldOperations>>> inputSteps = Multimaps.asMap( sourceLineageMap );
    List<PushDownOptimizationMeta> optimizationList = Lists.newArrayListWithExpectedSize( inputSteps.size() );
    for ( Map.Entry<String, Set<List<StepFieldOperations>>> inputStepLineage : inputSteps.entrySet() ) {
      String inputStep = inputStepLineage.getKey();
      Set<List<StepFieldOperations>> lineageSet = inputStepLineage.getValue();
      PushDownOptimizationMeta pushDownOptimizationMeta = new PushDownOptimizationMeta();
      ParameterGeneration parameterGeneration = serviceProvider.createPushDown();
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

  @Override public Set<Class<? extends PushDownType>> getProvidedOptimizationTypes() {
    return ImmutableSet.<Class<? extends PushDownType>>of( ParameterGeneration.class );
  }
}
