package com.pentaho.di.trans.dataservice.optimization.paramgen;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.pentaho.di.trans.dataservice.DataServiceMeta;
import com.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import com.pentaho.di.trans.dataservice.optimization.PushDownType;
import com.pentaho.metaverse.api.ILineageClient;
import com.pentaho.metaverse.client.StepFieldOperations;
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
  final private ParameterGenerationServiceProvider serviceProvider;

  public AutoParameterGenerationService( ILineageClient lineageClient,
                                         ParameterGenerationServiceProvider serviceProvider ) {
    this.lineageClient = lineageClient;
    this.serviceProvider = serviceProvider;
  }

  public AutoParameterGenerationService( ILineageClient lineageClient ) {
    this.lineageClient = lineageClient;
    serviceProvider = new ParameterGenerationServiceProvider();
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
