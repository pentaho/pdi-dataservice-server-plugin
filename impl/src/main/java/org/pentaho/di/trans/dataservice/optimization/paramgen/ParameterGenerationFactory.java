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
