/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2023 by Hitachi Vantara : http://www.pentaho.com
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
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.parameters.DuplicateParamException;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.core.sql.SQLCondition;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.OptimizationImpactInfo;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownType;
import org.pentaho.metastore.persist.MetaStoreAttribute;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author nhudak
 */
public class ParameterPushdown implements PushDownType {
  static final String NAME = "Parameter Capture";

  public static final String DEFINITIONS_LIST_ATTRIBUTE = "definitions_list";
  public static final String PARAMETER_ATTRIBUTE = "parameter_name";
  public static final String FIELD_NAME_ATTRIBUTE = "column_name";
  public static final String FORMAT_ATTRIBUTE = "parameter_format";
  public static final String DEFAULT_FORMAT = "%s";
  public static final String PARAMETER_PREFIX = "_QUERY";

  private static final Joiner.MapJoiner mapJoiner = Joiner.on( '\n' ).withKeyValueSeparator( " = " ).useForNull( "" );

  @MetaStoreAttribute( key = DEFINITIONS_LIST_ATTRIBUTE )
  private final List<Definition> definitions = Lists.newArrayList();

  public List<Definition> getDefinitions() {
    return definitions;
  }

  public Definition createDefinition() {
    Definition definition = new Definition();
    definitions.add( definition );
    return definition;
  }

  @Override public void init( TransMeta transMeta, DataServiceMeta dataService, PushDownOptimizationMeta optMeta ) {
    optMeta.setStepName( dataService.getStepname() );

    for ( Iterator<Definition> iterator = definitions.iterator(); iterator.hasNext(); ) {
      Definition definition = iterator.next();
      if ( definition.isValid() ) {
        try {
          transMeta.addParameterDefinition( definition.getParameter(), "", "Data Service Field Parameter" );
        } catch ( DuplicateParamException e ) {
          // Ignore duplicates
        }
      } else {
        iterator.remove();
      }
    }
    transMeta.activateParameters();
  }

  @Override
  public ListenableFuture<Boolean> activate( DataServiceExecutor executor, PushDownOptimizationMeta meta ) {
    Map<String, String> parameterValues = captureParameterValues( executor.getSql() );

    executor.getParameters().putAll( parameterValues );
    return Futures.immediateFuture( !parameterValues.isEmpty() );
  }

  private static final ImmutableSet<Integer> ALLOWED_FUNCTIONS = ImmutableSet.of(
    Condition.FUNC_EQUAL
  );

  private static final ImmutableSet<Integer> ALLOWED_OPERATORS = ImmutableSet.of(
    Condition.OPERATOR_NONE,
    Condition.OPERATOR_AND
  );

  @VisibleForTesting
  protected Map<String, String> captureParameterValues( SQL sql ) {
    Optional<SQLCondition> whereCondition = Optional.fromNullable( sql.getWhereCondition() );
    Multimap<String, Condition> conditionMap = FluentIterable.from( whereCondition.asSet() )
      .transformAndConcat( new Function<SQLCondition, Iterable<Condition>>() {
        @Override public Iterable<Condition> apply( SQLCondition sqlCondition ) {
          Condition condition = sqlCondition.getCondition();
          condition.simplify();

          // Flatten first level of conditions
          if ( !condition.isComposite() ) {
            return Collections.singleton( condition );
          }

          // All child conditions must have allowable operators
          for ( Condition child : condition.getChildren() ) {
              if ( !ALLOWED_OPERATORS.contains( child.getOperator() ) ) {
                return Collections.emptySet();
              }
          }

          return condition.getChildren();
        }
      } )
      .filter( new Predicate<Condition>() {
        @Override public boolean apply( Condition condition ) {
          // Only simple 'equals' conditions should be allowed
          // in the form of: WHERE A = 1
          return !condition.isComposite() && !condition.isNegated() && condition.getRightExact() != null
            && ALLOWED_FUNCTIONS.contains( condition.getFunction() );
        }
      } )
      .index( new Function<Condition, String>() {
        @Override public String apply( Condition condition ) {
          // Group by field for easy look up
          return condition.getLeftValuename();
        }
      } );

    Map<String, String> builder = Maps.newLinkedHashMap();
    for ( Definition definition : definitions ) {
      // There should be either 0 or 1 conditions for each field.
      for ( Condition condition : conditionMap.get( definition.getFieldName() ) ) {
        builder.put( definition.getParameter(), definition.format( condition ) );
      }
    }
    return ImmutableMap.copyOf( builder );
  }

  @Override public OptimizationImpactInfo preview( DataServiceExecutor executor, PushDownOptimizationMeta meta ) {
    OptimizationImpactInfo impactInfo = new OptimizationImpactInfo( meta.getStepName() );

    try {
      TransMeta serviceTrans = executor.getServiceTransMeta();
      Map<String, String> defaults = Maps.newLinkedHashMap();
      for ( Definition definition : definitions ) {
        String defaultValue = serviceTrans.getParameterDefault( definition.getParameter() );
        defaults.put( definition.getParameter(), MoreObjects.firstNonNull( defaultValue, "" ) );
      }

      Map<String, String> parameterValues = Maps.newLinkedHashMap( defaults );
      parameterValues.putAll( captureParameterValues( executor.getSql() ) );

      impactInfo.setQueryBeforeOptimization( mapJoiner.join( defaults ) );
      impactInfo.setQueryAfterOptimization( mapJoiner.join( parameterValues ) );
      impactInfo.setModified( !parameterValues.equals( defaults ) );
    } catch ( Exception e ) {
      impactInfo.setErrorMsg( e );
    }

    return impactInfo;
  }

  public static class Definition {

    @MetaStoreAttribute( key = PARAMETER_ATTRIBUTE )
    private String parameter;

    @MetaStoreAttribute( key = FORMAT_ATTRIBUTE )
    private String format = DEFAULT_FORMAT;

    @MetaStoreAttribute( key = FIELD_NAME_ATTRIBUTE )
    private String fieldName;

    public String getFieldName() {
      return fieldName;
    }

    public Definition setFieldName( String column ) {
      this.fieldName = column;
      return this;
    }

    public String getParameter() {
      return parameter;
    }

    public Definition setParameter( String parameter ) {
      this.parameter = parameter;
      return this;
    }

    public String getFormat() {
      if ( Strings.isNullOrEmpty( format ) ) {
        format = DEFAULT_FORMAT;
      }
      return format;
    }

    public void setFormat( String format ) {
      this.format = format;
    }

    private String format( Condition condition ) {
      return String.format( getFormat(), condition.getRightExact().getValueData().toString().trim() );
    }

    @Override public String toString() {
      return MoreObjects.toStringHelper( this )
        .add( "fieldName", getFieldName() )
        .add( "parameter", getParameter() )
        .add( "format", getFormat() )
        .toString();
    }

    public boolean isValid() {
      return !Strings.isNullOrEmpty( getParameter() ) && !Strings.isNullOrEmpty( getFieldName() );
    }
  }
}
