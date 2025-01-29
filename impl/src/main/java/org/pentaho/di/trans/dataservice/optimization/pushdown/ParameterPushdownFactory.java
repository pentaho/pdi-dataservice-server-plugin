/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
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
