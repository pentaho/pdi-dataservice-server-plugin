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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.SourceTargetFields;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGeneration;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGenerationFactory;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.di.trans.step.StepMeta;

import java.beans.PropertyChangeSupport;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class ParameterGenerationModelTest {

  @Mock PropertyChangeSupport changeSupport;
  @Mock DataServiceModel dialogModel;
  @Mock ImmutableMap<String, StepMeta> supportedSteps;
  @Mock ParameterGenerationFactory factory;

  ParameterGenerationModel model;
  private List<PushDownOptimizationMeta> optimizations;

  @Before
  public void setUp() throws Exception {
    model = new ParameterGenerationModel( factory, dialogModel ) {
      {
        changeSupport = ParameterGenerationModelTest.this.changeSupport;
      }
    };

    optimizations = Lists.newArrayList();
    when( dialogModel.getPushDownOptimizations( ParameterGeneration.class ) )
      .then( new Answer<List<PushDownOptimizationMeta>>() {
        @Override public List<PushDownOptimizationMeta> answer( InvocationOnMock invocation ) throws Throwable {
          return ImmutableList.copyOf( optimizations );
        }
      } );

    when( factory.createSourceTargetAdapter( any( SourceTargetFields.class ) ) )
      .then( new Answer<SourceTargetAdapter>() {
        @Override public SourceTargetAdapter answer( InvocationOnMock invocation ) throws Throwable {
          return new SourceTargetAdapter( (SourceTargetFields) invocation.getArguments()[0] );
        }
      } );
  }

  @Test
  public void testParameterSelection() throws Exception {
    Map<String, PushDownOptimizationMeta> parameterMap = Maps.newHashMap();

    assertThat( model.getSelectedParameter(), nullValue() );
    assertThat( model.getSelectedOptimization(), nullValue() );
    assertThat( model.getParameterGeneration(), nullValue() );
    assertThat( model.getParameterMap(), anEmptyMap() );

    PushDownOptimizationMeta first = createParameterGeneration( "first" );
    parameterMap.put( "first", first );

    model.setParameterMap( parameterMap );
    assertThat( model.getParameterMap(), is( parameterMap ) );
    assertThat( model.getSelectedParameter(), nullValue() );
    verify( changeSupport ).firePropertyChange( "parameterMap", ImmutableMap.of(), parameterMap );

    reset( changeSupport );
    model.setSelectedParameter( "first" );

    assertThat( model.getSelectedParameter(), is( "first" ) );
    assertThat( model.getSelectedOptimization(), sameInstance( first ) );
    assertThat( model.getParameterGeneration(), sameInstance( first.getType() ) );
    verify( changeSupport ).firePropertyChange( "selectedParameter", null, "first" );

    reset( changeSupport );
    PushDownOptimizationMeta second = createParameterGeneration( "second" );
    parameterMap.put( "second", second );
    model.add( second );
    verify( dialogModel ).add( second );
    assertThat( model.getParameterMap(), is( parameterMap ) );

    assertThat( model.getParameterMap(), is( ImmutableMap.of(
      "first", first,
      "second", second
    ) ) );
    verify( changeSupport ).firePropertyChange( "parameterMap", ImmutableMap.of( "first", first ), parameterMap );
    verify( changeSupport ).firePropertyChange( "selectedParameter", null, "first" );

    reset( changeSupport );
    model.setSelectedParameter( "second" );

    assertThat( model.getSelectedParameter(), is( "second" ) );
    assertThat( model.getSelectedOptimization(), sameInstance( second ) );
    assertThat( model.getParameterGeneration(), sameInstance( second.getType() ) );
    verify( changeSupport ).firePropertyChange( "selectedParameter", "first", "second" );

    reset( changeSupport );
    when( dialogModel.remove( second ) ).thenReturn( true );
    optimizations.remove( second );
    parameterMap.remove( "second" );
    model.remove( second );

    verify( dialogModel ).remove( second );

    assertThat( model.getParameterMap(), is( parameterMap ) );
    assertThat( model.getSelectedParameter(), nullValue() );
    assertThat( model.getSelectedOptimization(), nullValue() );
    assertThat( model.getParameterGeneration(), nullValue() );
    verify( changeSupport ).firePropertyChange( "selectedParameter", "second", null );
  }

  private PushDownOptimizationMeta createParameterGeneration( String parameterName ) {
    PushDownOptimizationMeta meta = new PushDownOptimizationMeta();
    ParameterGeneration parameterGeneration = new ParameterGeneration( factory );
    meta.setType( parameterGeneration );
    parameterGeneration.setParameterName( parameterName );
    optimizations.add( meta );
    return meta;
  }

  @Test
  public void testGetDialogModel() throws Exception {
    assertThat( model.getDialogModel(), sameInstance( dialogModel ) );
  }

  @Test
  public void testGetSelectedStep() throws Exception {
    createParameterGeneration( "parameter" ).setStepName( "firstStep" );
    model.updateParameterMap();

    assertThat( model.getSelectedStep(), nullValue() );

    model.setSelectedParameter( "parameter" );
    assertThat( model.getSelectedStep(), is( "firstStep" ) );
    verify( changeSupport ).firePropertyChange( "selectedStep", null, "firstStep" );

    model.setSelectedStep( "secondStep" );
    assertThat( model.getSelectedStep(), is( "secondStep" ) );
    assertThat( model.getSelectedOptimization().getStepName(), is( "secondStep" ) );
    verify( changeSupport ).firePropertyChange( "selectedStep", "firstStep", "secondStep" );
  }

  @Test
  public void testIsEnabled() throws Exception {
    createParameterGeneration( "parameter" ).setEnabled( false );
    model.updateParameterMap();

    assertThat( model.isEnabled(), is( false ) );

    model.setSelectedParameter( "parameter" );
    assertThat( model.isEnabled(), is( false ) );
    verify( changeSupport ).firePropertyChange( "enabled", null, Boolean.FALSE );

    model.setEnabled( true );
    assertThat( model.isEnabled(), is( true ) );
    assertThat( model.getSelectedOptimization().isEnabled(), is( true ) );
    verify( changeSupport ).firePropertyChange( "enabled", Boolean.FALSE, Boolean.TRUE );
  }

  @Test
  public void testGetMappings() throws Exception {
    ParameterGeneration parameterGeneration = (ParameterGeneration) createParameterGeneration( "parameter" ).getType();
    parameterGeneration.createFieldMapping( "source", "target" );
    model.updateParameterMap();

    assertThat( model.getMappings(), empty() );

    model.setSelectedParameter( "parameter" );
    ImmutableList<SourceTargetAdapter> mappings = model.getMappings();
    assertThat( mappings, hasSize( greaterThan( 1 ) ) );
    assertThat( mappings.get( 0 ), allOf(
      hasProperty( "sourceFieldName", is( "source" ) ),
      hasProperty( "targetFieldName", is( "target" ) )
    ) );
    assertThat( mappings.subList( 1, mappings.size() ), everyItem( hasProperty( "defined", is( false ) ) ) );
    verify( changeSupport ).firePropertyChange( "mappings", ImmutableList.of(), mappings );

    SourceTargetAdapter sourceTargetAdapter = mappings.get( 1 );
    sourceTargetAdapter.setSourceFieldName( "secondSource" );
    sourceTargetAdapter.setTargetFieldName( "secondTarget" );

    assertThat( parameterGeneration.getFieldMappings().get( 1 ).getSourceFieldName(), is( "secondSource" ) );
    assertThat( parameterGeneration.getFieldMappings().get( 1 ).getTargetFieldName(), is( "secondTarget" ) );
  }
}
