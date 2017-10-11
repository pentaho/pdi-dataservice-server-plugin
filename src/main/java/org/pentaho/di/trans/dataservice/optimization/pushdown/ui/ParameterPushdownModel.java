/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.optimization.pushdown.ui;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.pentaho.di.trans.dataservice.optimization.pushdown.ParameterPushdown;
import org.pentaho.di.trans.dataservice.ui.AbstractModel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * @author nhudak
 */
public class ParameterPushdownModel extends AbstractModel {
  private List<String> fieldList = new ArrayList<>();

  private final ParameterPushdown parameterPushdown;
  private ImmutableList<DefinitionAdapter> definitions = ImmutableList.of();

  public ParameterPushdownModel( ParameterPushdown parameterPushdown ) {
    this.parameterPushdown = parameterPushdown;
  }

  public ParameterPushdownModel reset() {
    final ImmutableList.Builder<DefinitionAdapter> builder = ImmutableList.builder();

    Iterator<ParameterPushdown.Definition> iterator = parameterPushdown.getDefinitions().iterator();
    while ( iterator.hasNext() ) {
      ParameterPushdown.Definition definition = iterator.next();
      if ( definition.isValid() ) {
        builder.add( createAdapter( definition ) );
      } else {
        iterator.remove();
      }
    }

    for ( int i = 0; i < 10; i++ ) {
      builder.add( createAdapter( parameterPushdown.createDefinition() ) );
    }

    modify( new Runnable() {
      @Override public void run() {
        definitions = builder.build();
      }
    } );

    return this;
  }

  public ParameterPushdown getDelegate() {
    return parameterPushdown;
  }

  public ImmutableList<DefinitionAdapter> getDefinitions() {
    return definitions;
  }

  public void setFieldList( ImmutableList<String> fieldList ) {
    this.fieldList = fieldList;
    for ( DefinitionAdapter definitionAdapter : definitions ) {
      definitionAdapter.setFieldList( fieldList );
    }
    firePropertyChange( "definitions", null, definitions );
  }

  @Override public Map<String, Object> snapshot() {
    return ImmutableMap.of(
      "definitions", (Object) getDefinitions()
    );
  }

  @VisibleForTesting
  protected DefinitionAdapter createAdapter( ParameterPushdown.Definition definition ) {
    DefinitionAdapter definitionAdapter = new DefinitionAdapter( definition );
    definitionAdapter.setFieldList( fieldList );
    return definitionAdapter;
  }

  public class DefinitionAdapter extends AbstractModel {

    private final ParameterPushdown.Definition definition;
    private List<String> fieldList;

    public String getFieldName() {
      return Strings.nullToEmpty( definition.getFieldName() );
    }

    public String getParameter() {
      if ( !Strings.isNullOrEmpty( definition.getFieldName() ) && ( Strings.isNullOrEmpty( definition.getParameter() )
          || definition.getParameter().endsWith( ParameterPushdown.PARAMETER_PREFIX ) ) ) {
        definition.setParameter( definition.getFieldName().toUpperCase() + ParameterPushdown.PARAMETER_PREFIX );
      }
      return Strings.nullToEmpty( definition.getParameter() );
    }

    public void setFieldList( List<String> fieldList ) {
      this.fieldList = fieldList;
    }

    public Vector<String> getFieldList() {
      Vector<String> v = new Vector<>();
      v.add( "" );
      v.addAll( fieldList );
      return v;
    }

    public String getFormat() {
      // If other fields are empty, leave format blank
      return isEmpty() ? "" : Strings.nullToEmpty( definition.getFormat() );
    }

    public boolean isEmpty() {
      return getFieldName().isEmpty() && getParameter().isEmpty();
    }

    public void setFieldName( final String fieldName ) {
      modify( new Runnable() {
        @Override public void run() {
          definition.setFieldName( fieldName );
        }
      } );
    }

    public void setParameter( final String parameter ) {
      modify( new Runnable() {
        @Override public void run() {
          definition.setParameter( parameter );
        }
      } );
    }

    public void setFormat( final String format ) {
      modify( new Runnable() {
        @Override public void run() {
          definition.setFormat( format );
        }
      } );
    }

    private DefinitionAdapter( ParameterPushdown.Definition definition ) {
      this.definition = definition;
    }

    @Override public Map<String, Object> snapshot() {
      return ImmutableMap.<String, Object>of(
        "parameter", getParameter(),
        "format", getFormat(),
        "fieldName", getFieldName()
      );
    }
  }
}
