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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.pentaho.di.trans.dataservice.DataServiceMeta;
import com.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import com.pentaho.di.trans.dataservice.optimization.PushDownType;
import com.pentaho.metaverse.api.ILineageClient;
import com.pentaho.metaverse.api.StepFieldOperations;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  public AutoParameterGenerationService( ILineageClient lineageClient ) {
    this.lineageClient = lineageClient;
    serviceProvider = new ParameterGenerationFactory();
  }

  @Override public List<PushDownOptimizationMeta> apply( TransMeta transMeta, DataServiceMeta dataServiceMeta ) {
    LogChannelInterface logChannel = transMeta.getLogChannel() != null ? transMeta.getLogChannel() : LogChannel.GENERAL;
    try {
      String stepName = dataServiceMeta.getStepname();
      List<String> fields = Arrays.asList( transMeta.getStepFields( stepName ).getFieldNames() );
      Map<String, Set<List<StepFieldOperations>>> operationPaths = lineageClient.getOperationPaths( transMeta, stepName, fields );
      return convertToTable( operationPaths ).
        filterChangedValues().
        filterSupportedInputs( transMeta, serviceProvider ).
        filterExistingOptimizations( dataServiceMeta ).
        generateOptimizationList();
    } catch ( Throwable e ) {
      logChannel.logError( "Unable to run Auto-Optimization", e );
      return Collections.emptyList();
    }
  }

  protected SourceLineageMap convertToTable( Map<String, Set<List<StepFieldOperations>>> operationPaths ) {
    SourceLineageMap sourceLineageMap = SourceLineageMap.create();
    for ( List<StepFieldOperations> lineage : Iterables.concat( operationPaths.values() ) ) {
      if ( ( lineage != null ) && !lineage.isEmpty() ) {
        String inputStep = lineage.get( 0 ).getStepName();
        sourceLineageMap.put( inputStep, lineage );
      }
    }
    return sourceLineageMap;
  }

  @Override public Set<Class<? extends PushDownType>> getProvidedOptimizationTypes() {
    return ImmutableSet.<Class<? extends PushDownType>>of( ParameterGeneration.class );
  }
}
