/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.optimization.PushDownType;
import org.pentaho.di.trans.dataservice.optimization.SourceTargetFields;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ui.ParameterGenerationController;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ui.ParameterGenerationModel;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ui.ParameterGenerationOverlay;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ui.SourceTargetAdapter;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metaverse.api.ILineageClient;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author nhudak
 */
public class ParameterGenerationFactory implements PushDownFactory {

  private final List<ParameterGenerationServiceFactory> factories;
  private ILineageClient lineageClient;

  public ParameterGenerationFactory( List<ParameterGenerationServiceFactory> factories ) {
    this.factories = factories;
  }

  public ILineageClient getLineageClient() {
    return lineageClient;
  }

  public void setLineageClient( ILineageClient lineageClient ) {
    this.lineageClient = lineageClient;
  }

  @Override public String getName() {
    return ParameterGeneration.TYPE_NAME;
  }

  @Override public Class<? extends PushDownType> getType() {
    return ParameterGeneration.class;
  }

  @Override public ParameterGeneration createPushDown() {
    return new ParameterGeneration( this );
  }

  @Override public ParameterGenerationOverlay createOverlay() {
    return new ParameterGenerationOverlay( this );
  }

  public ParameterGenerationController createController( DataServiceModel dialogModel ) {
    ParameterGenerationModel model = new ParameterGenerationModel( this, dialogModel );
    return new ParameterGenerationController( this, model );
  }

  public SourceTargetAdapter createSourceTargetAdapter( SourceTargetFields sourceTargetFields ) {
    return new SourceTargetAdapter( sourceTargetFields );
  }

  public AutoParameterGenerationService createAutoOptimizationService() {
    return new AutoParameterGenerationService( checkNotNull( lineageClient, "Lineage Client is unavailable" ), this );
  }

  public ParameterGenerationService getService( StepMeta stepMeta ) {
    Optional<ParameterGenerationServiceFactory> factory = getFactory( stepMeta );
    return factory.isPresent() ? factory.get().getService( stepMeta ) : null;
  }

  public boolean supportsStep( StepMeta stepMeta ) {
    return getFactory( stepMeta ).isPresent();
  }

  private Optional<ParameterGenerationServiceFactory> getFactory( final StepMeta stepMeta ) {
    return Iterables.tryFind( factories, new Predicate<ParameterGenerationServiceFactory>() {
      @Override public boolean apply( ParameterGenerationServiceFactory input ) {
        return input.supportsStep( stepMeta );
      }
    } );
  }
}
