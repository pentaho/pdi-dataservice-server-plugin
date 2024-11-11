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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.SourceTargetFields;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGeneration;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGenerationFactory;
import org.pentaho.di.trans.dataservice.ui.AbstractModel;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author nhudak
 */
public class ParameterGenerationModel extends AbstractModel {
  private final ParameterGenerationFactory factory;
  private final DataServiceModel dialogModel;
  private ImmutableMap<String, PushDownOptimizationMeta> parameterMap = ImmutableMap.of();
  private List<SourceTargetAdapter> mappings = Lists.newArrayList();
  private String selectedParameter;

  public ParameterGenerationModel( ParameterGenerationFactory factory, DataServiceModel dialogModel ) {
    this.factory = factory;
    this.dialogModel = dialogModel;
  }

  public void add( PushDownOptimizationMeta meta ) {
    dialogModel.add( meta );
    updateParameterMap();
  }

  public void remove( PushDownOptimizationMeta meta ) {
    if ( dialogModel.remove( meta ) ) {
      updateParameterMap();
    }
  }

  public ImmutableMap<String, PushDownOptimizationMeta> getParameterMap() {
    return parameterMap;
  }

  protected void updateParameterMap() {
    ImmutableList<PushDownOptimizationMeta> list = dialogModel.getPushDownOptimizations( ParameterGeneration.class );
    Map<String, PushDownOptimizationMeta> map = Maps.newHashMapWithExpectedSize( list.size() );
    for ( PushDownOptimizationMeta meta : list ) {
      ParameterGeneration parameterGeneration = (ParameterGeneration) meta.getType();
      String parameterName = parameterGeneration.getParameterName();

      // If parameter already exists, add a unique suffix
      int offset = 0;
      while ( map.containsKey( parameterName ) ) {
        parameterName = String.format( "%s_%d", parameterGeneration.getParameterName(), ++offset );
      }
      if ( offset > 0 ) {
        parameterGeneration.setParameterName( parameterName );
      }

      map.put( parameterName, meta );
    }

    setParameterMap( map );
    if ( !map.containsKey( getSelectedParameter() ) ) {
      setSelectedParameter( null );
    }
  }

  public void setParameterMap( Map<String, PushDownOptimizationMeta> map ) {
    ImmutableMap<String, PushDownOptimizationMeta> previous = parameterMap;

    parameterMap = ImmutableMap.copyOf( map );

    firePropertyChange( "parameterMap", previous, parameterMap );
    // Force an update
    firePropertyChanges( ImmutableMap.<String, Object>of() );
  }

  public String getSelectedParameter() {
    return selectedParameter;
  }

  public PushDownOptimizationMeta getSelectedOptimization() {
    return selectedParameter != null ? parameterMap.get( selectedParameter ) : null;
  }

  public ParameterGeneration getParameterGeneration() {
    PushDownOptimizationMeta meta = getSelectedOptimization();
    return meta == null ? null : (ParameterGeneration) meta.getType();
  }

  public void setSelectedParameter( final String selectedParameter ) {
    if ( Objects.equals( selectedParameter, this.selectedParameter ) ) {
      return;
    }
    final ParameterGenerationModel model = this;
    modify( new Runnable() {
      public void run() {
        model.selectedParameter = selectedParameter;
        resetMappings( getParameterGeneration() );
      }
    } );
  }

  @Override public Map<String, Object> snapshot() {
    Map<String, Object> map = Maps.newHashMap();
    map.put( "selectedParameter", getSelectedParameter() );
    map.put( "selectedStep", getSelectedStep() );
    map.put( "enabled", isEnabled() );
    map.put( "mappings", getMappings() );
    return map;
  }

  public DataServiceModel getDialogModel() {
    return dialogModel;
  }

  public String getSelectedStep() {
    PushDownOptimizationMeta meta = getSelectedOptimization();
    return meta != null ? meta.getStepName() : null;
  }

  public void setSelectedStep( String stepName ) {
    PushDownOptimizationMeta meta = getSelectedOptimization();
    if ( meta != null ) {
      String previous = meta.getStepName();
      meta.setStepName( stepName );
      firePropertyChange( "selectedStep", previous, stepName );
    }
  }

  public void setEnabled( boolean enabled ) {
    PushDownOptimizationMeta meta = getSelectedOptimization();
    if ( meta != null ) {
      boolean previous = meta.isEnabled();
      meta.setEnabled( enabled );
      firePropertyChange( "enabled", previous, enabled );
    }
  }

  public boolean isEnabled() {
    PushDownOptimizationMeta meta = getSelectedOptimization();
    return meta != null && meta.isEnabled();
  }

  private void resetMappings( ParameterGeneration parameterGeneration ) {
    mappings = Lists.newArrayList();
    if ( parameterGeneration != null ) {
      // Add mapping from current optimization
      Iterator<SourceTargetFields> iterator = parameterGeneration.getFieldMappings().iterator();
      while ( iterator.hasNext() ) {
        SourceTargetFields sourceTargetFields = iterator.next();
        if ( sourceTargetFields.isDefined() ) {
          mappings.add( factory.createSourceTargetAdapter( sourceTargetFields ) );
        } else {
          iterator.remove();
        }
      }
      // Unable to get the tree to grow, just adding a bunch of blanks
      //TODO Automatically grow model without getting XUL errors
      for ( int i = 0; i < 10; i++ ) {
        mappings.add( factory.createSourceTargetAdapter( parameterGeneration.createFieldMapping() ) );
      }
    }
  }

  public ImmutableList<SourceTargetAdapter> getMappings() {
    return ImmutableList.copyOf( mappings );
  }

}
