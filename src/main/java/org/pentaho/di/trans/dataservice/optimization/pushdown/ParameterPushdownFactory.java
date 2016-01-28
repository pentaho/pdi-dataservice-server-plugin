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

package org.pentaho.di.trans.dataservice.optimization.pushdown;

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.pushdown.ui.ParameterPushdownController;
import org.pentaho.di.trans.dataservice.optimization.pushdown.ui.ParameterPushdownModel;
import org.pentaho.di.trans.dataservice.ui.DataServiceDialog;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.List;

/**
 * @author nhudak
 */
public class ParameterPushdownFactory implements PushDownFactory {
  @VisibleForTesting
  static final String XUL_SOURCE = "/org/pentaho/di/trans/dataservice/optimization/pushdown/ui/overlay.xul";

  @Override public String getName() {
    return ParameterPushdown.NAME;
  }

  @Override public Class<ParameterPushdown> getType() {
    return ParameterPushdown.class;
  }

  @Override public ParameterPushdown createPushDown() {
    return new ParameterPushdown();
  }

  @Override public DataServiceDialog.OptimizationOverlay createOverlay() {
    return new DataServiceDialog.OptimizationOverlay() {
      @Override public double getPriority() {
        return 2;
      }

      @Override public void apply( final DataServiceDialog dialog ) throws KettleException {
        ParameterPushdownController controller = createController( dialog.getModel() );
        dialog.applyOverlay( this, XUL_SOURCE ).addEventHandler( controller );
      }
    };
  }

  protected ParameterPushdownController createController( DataServiceModel dialogModel ) {
    return new ParameterPushdownController( createModel( dialogModel ) );
  }

  public ParameterPushdownModel createModel( final DataServiceModel dialogModel ) {
    List<PushDownOptimizationMeta> optimizations = dialogModel.getPushDownOptimizations( ParameterPushdown.class );

    ParameterPushdown parameterPushdown;
    if ( optimizations.isEmpty() ) {
      parameterPushdown = createPushDown();

      PushDownOptimizationMeta optimizationMeta = new PushDownOptimizationMeta();
      optimizationMeta.setStepName( dialogModel.getServiceStep() );
      optimizationMeta.setType( parameterPushdown );

      dialogModel.add( optimizationMeta );
    } else {
      parameterPushdown = (ParameterPushdown) optimizations.get( 0 ).getType();
    }

    if ( optimizations.size() > 1 ) {
      dialogModel.removeAll( optimizations.subList( 1, optimizations.size() ) );
    }

    final ParameterPushdownModel parameterPushdownModel = new ParameterPushdownModel( parameterPushdown );
    parameterPushdownModel.setFieldList( dialogModel.getStepFields() );
    dialogModel.addPropertyChangeListener( "serviceStep", new PropertyChangeListener() {
      @Override public void propertyChange( PropertyChangeEvent propertyChangeEvent ) {
        parameterPushdownModel.setFieldList( dialogModel.getStepFields() );
      }
    } );

    return parameterPushdownModel;
  }
}
