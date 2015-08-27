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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.SourceTargetFields;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGeneration;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.ui.xul.XulEventSourceAdapter;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author nhudak
 */
public class ParameterGenerationModel extends XulEventSourceAdapter {
  private final DataServiceModel dialogModel;
  private final ImmutableMap<String, StepMeta> supportedSteps;
  private ImmutableMap<String, PushDownOptimizationMeta> parameterMap = ImmutableMap.of();
  private List<SourceTargetAdapter> mappings = Lists.newArrayList();
  private String selectedParameter;

  public ParameterGenerationModel( DataServiceModel dialogModel, ImmutableMap<String, StepMeta> supportedSteps ) {
    this.dialogModel = dialogModel;
    this.supportedSteps = supportedSteps;
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
    ImmutableMap<String, PushDownOptimizationMeta> previous = parameterMap;

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

    parameterMap = ImmutableMap.copyOf( map );

    firePropertyChange( "parameterMap", previous, parameterMap );
    if ( !parameterMap.containsKey( selectedParameter ) ) {
      selectedParameter =  null;
    }
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

  public void setSelectedParameter( String selectedParameter ) {
    if ( Objects.equals( selectedParameter, this.selectedParameter ) ) {
      return;
    }
    Map<String, Object> previous = snapshot();

    this.selectedParameter = selectedParameter;
    resetMappings( getParameterGeneration() );

    // Fire property change for all derived properties
    firePropertyChanges( previous );
  }

  private Map<String, Object> snapshot() {
    Map<String, Object> map = Maps.newHashMap();
    map.put( "selectedParameter", getSelectedParameter() );
    map.put( "selectedStep", getSelectedStep() );
    map.put( "enabled", isEnabled() );
    map.put( "mappings", getMappings() );
    return map;
  }

  private void firePropertyChanges( Map<String, Object> previous ) {
    for ( Map.Entry<String, Object> entry : snapshot().entrySet() ) {
      String attr = entry.getKey();
      firePropertyChange( attr, previous.get( attr ), entry.getValue() );
    }
  }

  public ImmutableMap<String, StepMeta> getSupportedSteps() {
    return supportedSteps;
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
          mappings.add( new SourceTargetAdapter( sourceTargetFields ) );
        } else {
          iterator.remove();
        }
      }
      // Unable to get the tree to grow, just just adding a bunch of blanks
      //TODO Automatically grow model without getting XUL errors
      for ( int i = 0; i < 10; i++ ) {
        mappings.add( new SourceTargetAdapter( parameterGeneration.createFieldMapping() ) );
      }
    }
  }

  public ImmutableList<SourceTargetAdapter> getMappings() {
    return ImmutableList.copyOf( mappings );
  }

}
