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


package org.pentaho.di.trans.dataservice.optimization.paramgen.ui;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGenerationFactory;
import org.pentaho.di.trans.dataservice.ui.DataServiceDialog;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.di.trans.step.StepMeta;

/**
 * @author nhudak
 */
public class ParameterGenerationOverlay implements DataServiceDialog.OptimizationOverlay {
  private static final String XUL_OVERLAY =
    "org/pentaho/di/trans/dataservice/optimization/paramgen/ui/param-gen-overlay.xul";

  private ParameterGenerationFactory factory;

  public ParameterGenerationOverlay( ParameterGenerationFactory factory ) {
    this.factory = factory;
  }

  @Override public double getPriority() {
    return 1.0;
  }

  @Override public void apply( DataServiceDialog dialog ) throws KettleException {
    DataServiceModel dialogModel = dialog.getModel();
    TransMeta transMeta = dialogModel.getTransMeta();

    ParameterGenerationController controller = factory.createController( dialogModel );

    dialog
      .applyOverlay( this, XUL_OVERLAY )
      .addEventHandler( controller );

    controller.initBindings( findSupportedSteps( transMeta ).keySet().asList() );
  }

  protected ImmutableMap<String, StepMeta> findSupportedSteps( TransMeta transMeta ) {
    return FluentIterable.from( transMeta.getSteps() )
      .filter( new Predicate<StepMeta>() {
        @Override public boolean apply( StepMeta input ) {
          return factory.supportsStep( input );
        }
      } )
      .uniqueIndex( new Function<StepMeta, String>() {
        @Override public String apply( StepMeta input ) {
          return input.getName();
        }
      } );
  }
}
