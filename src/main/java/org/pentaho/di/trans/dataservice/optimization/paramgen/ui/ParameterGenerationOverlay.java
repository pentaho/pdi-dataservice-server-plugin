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

    ParameterGenerationModel model = new ParameterGenerationModel( dialogModel, findSupportedSteps( transMeta ) );
    ParameterGenerationController controller = new ParameterGenerationController( factory, model );

    dialog
      .applyOverlay( this, XUL_OVERLAY )
      .addEventHandler( controller );

    controller.initBindings();
  }

  protected ImmutableMap<String,StepMeta> findSupportedSteps( TransMeta transMeta ) {
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
