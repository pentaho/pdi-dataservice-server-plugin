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


package org.pentaho.di.trans.dataservice.optimization.pushdown.ui;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.hamcrest.MockitoHamcrest;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.trans.dataservice.optimization.pushdown.ParameterPushdown;

import java.beans.PropertyChangeSupport;
import java.util.Vector;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class ParameterPushdownModelTest {
  ParameterPushdown parameterPushdown;
  ParameterPushdownModel model;

  @Mock PropertyChangeSupport changeSupport;

  @Before
  public void setUp() throws Exception {
    parameterPushdown = new ParameterPushdown();
    model = new ParameterPushdownModel( parameterPushdown );

    model.setChangeSupport( changeSupport );
  }

  @Test
  public void testReset() throws Exception {
    ParameterPushdown.Definition definition = parameterPushdown.createDefinition();
    definition.setFieldName( "field" );
    definition.setParameter( "param" );

    ImmutableList<ParameterPushdownModel.DefinitionAdapter> definitions = model.reset().getDefinitions();
    verify( changeSupport ).firePropertyChange( eq( "definitions" ), MockitoHamcrest.argThat( empty() ), eq( definitions ) );

    assertThat( definitions, hasSize( greaterThan( 1 ) ) );
    assertThat( definitions.get( 0 ), equalsDefinition( "field", "param", ParameterPushdown.DEFAULT_FORMAT ) );
    assertThat( FluentIterable.from( definitions ).skip( 1 ), everyItem( equalsDefinition( "", "", "" ) ) );
  }

  public Matcher<Object> equalsDefinition( String fieldName, String parameter, String format ) {
    return allOf(
      hasProperty( "fieldName", is( fieldName ) ),
      hasProperty( "parameter", is( parameter ) ),
      hasProperty( "format", is( format ) )
    );
  }

  @Test
  public void testDefinitionAdapter() throws Exception {
    ParameterPushdown.Definition definition = parameterPushdown.createDefinition();
    ParameterPushdownModel.DefinitionAdapter adapter = model.createAdapter( definition );
  }

  @Test
  public void testSetFieldList() throws Exception {
    model.reset();
    model.setFieldList( ImmutableList.of( "field1", "field2", "field3" ) );

    Vector<String> fieldList = model.getDefinitions().get( 0 ).getFieldList();
    assertThat( fieldList.get( 0 ), equalTo( "" ) );
    assertThat( fieldList.get( 1 ), equalTo( "field1" ) );
    assertThat( fieldList.get( 2 ), equalTo( "field2" ) );
    assertThat( fieldList.get( 3 ), equalTo( "field3" ) );
  }

  @Test
  public void testGetParameter() throws Exception {
    model.reset();
    ParameterPushdownModel.DefinitionAdapter definitionAdapter =  model.getDefinitions().get( 0 );
    definitionAdapter.setFieldName( "field1" );
    definitionAdapter.setFieldList( ImmutableList.of( "field1", "field2", "field3" ) );

    assertThat( definitionAdapter.getParameter(),
        equalTo( definitionAdapter.getFieldName().toUpperCase() + ParameterPushdown.PARAMETER_PREFIX ) );
  }
}
