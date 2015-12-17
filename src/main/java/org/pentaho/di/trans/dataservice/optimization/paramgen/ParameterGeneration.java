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

package org.pentaho.di.trans.dataservice.optimization.paramgen;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.parameters.DuplicateParamException;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.OptimizationImpactInfo;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationException;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.SourceTargetFields;
import org.pentaho.di.trans.dataservice.optimization.StepOptimization;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.persist.MetaStoreAttribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author nhudak
 */
public class ParameterGeneration extends StepOptimization {

  public static final String PUSH_DOWN_FIELD_MAPPINGS = "field_mappings";
  public static final String PUSH_DOWN_PARAMETER_NAME = "parameter_name";
  public static final String TYPE_NAME = "Parameter Generation";

  @MetaStoreAttribute( key = PUSH_DOWN_FIELD_MAPPINGS )
  private List<SourceTargetFields> fieldMappings = new ArrayList<SourceTargetFields>();

  @MetaStoreAttribute( key = PUSH_DOWN_PARAMETER_NAME )
  private String parameterName;

  protected ParameterGenerationFactory serviceProvider;

  public ParameterGeneration( ParameterGenerationFactory serviceProvider ) {
    this.serviceProvider = serviceProvider;
  }

  public String getParameterName() {
    return parameterName == null ? "" : parameterName;
  }

  public void setParameterName( String parameterName ) {
    this.parameterName = parameterName;
  }

  public List<SourceTargetFields> getFieldMappings() {
    return fieldMappings;
  }

  public SourceTargetFields createFieldMapping() {
    SourceTargetFields mapping = new SourceTargetFields();
    fieldMappings.add( mapping );
    return mapping;
  }

  public SourceTargetFields createFieldMapping( String source, String target ) {
    SourceTargetFields mapping = new SourceTargetFields( source, target );
    fieldMappings.add( mapping );
    return mapping;
  }

  public SourceTargetFields removeFieldMapping( SourceTargetFields mapping ) {
    fieldMappings.remove( mapping );
    return mapping;
  }

  protected Condition mapConditionFields( Condition condition ) {
    Condition clone = (Condition) condition.clone();
    HashMap<String, SourceTargetFields> sourceMap;
    sourceMap = new HashMap<String, SourceTargetFields>();
    for ( SourceTargetFields fieldMapping : fieldMappings ) {
      sourceMap.put( fieldMapping.getSourceFieldName(), fieldMapping );
    }

    if ( applyMapping( clone, sourceMap ) ) {
      clone.simplify();
      return clone;
    } else {
      return null;
    }
  }

  private boolean applyMapping( Condition condition, Map<String, SourceTargetFields> sourceTargetFieldsMap ) {
    // Atomic: check for simple mapping
    if ( condition.isAtomic() ) {
      String key = condition.getLeftValuename();
      SourceTargetFields mapping = sourceTargetFieldsMap.get( key );
      if ( mapping != null ) {
        condition.setLeftValuename( mapping.getTargetFieldName() );
        return true;
      } else {
        return false;
      }
    } else {
      // Composite: decide if all child conditions are required
      List<Condition> children = condition.getChildren();
      int requireAllOp = condition.isNegated() ? Condition.OPERATOR_AND : Condition.OPERATOR_OR;
      boolean requireAll = false;
      for ( Condition child : children ) {
        if ( child.getOperator() == requireAllOp ) {
          requireAll = true;
          break;
        }
      }

      // Map each child
      for ( Iterator<Condition> i = children.iterator(); i.hasNext(); ) {
        Condition child = i.next();
        if ( !applyMapping( child, sourceTargetFieldsMap ) ) {
          if ( requireAll ) {
            // If all were required, give up now
            return false;
          } else {
            i.remove();
          }
        }
      }

      // Successful if any children were mapped
      return !children.isEmpty();
    }
  }

  @Override public void init( TransMeta transMeta, DataServiceMeta dataService, PushDownOptimizationMeta optMeta ) {
    // Ensure all source -> target mappings are well defined
    fieldMappings = Lists.newArrayList( Iterables.filter( fieldMappings, SourceTargetFields.IS_DEFINED ) );

    StepMeta stepMeta = transMeta.findStep( optMeta.getStepName() );
    ParameterGenerationService service = stepMeta != null ? serviceProvider.getService( stepMeta ) : null;
    String parameterDefault = service != null ? service.getParameterDefault() : "";

    String description = String.format( "Auto-generated parameter for Push Down Optimization: %s", optMeta.getStepName() );
    try {
      transMeta.addParameterDefinition( getParameterName(), parameterDefault, description );
      transMeta.activateParameters();
    } catch ( DuplicateParamException e ) {
      // Ignore duplicates
    }
  }

  @Override public boolean activate( DataServiceExecutor executor, StepInterface stepInterface ) {
    ParameterGenerationService service = serviceProvider.getService( stepInterface.getStepMeta() );
    Condition pushDownCondition = getPushDownCondition( executor.getSql() );

    return handlePushDown( service, pushDownCondition, stepInterface );
  }

  @Override
  public OptimizationImpactInfo preview( DataServiceExecutor executor, StepInterface stepInterface ) {
    ParameterGenerationService service = serviceProvider.getService( stepInterface.getStepMeta() );
    Condition pushDownCondition = getPushDownCondition( executor.getSql() );

    return service.preview( pushDownCondition, this, stepInterface );
  }

  private boolean handlePushDown( ParameterGenerationService service,
                                  Condition pushDownCondition,
                                  StepInterface stepInterface ) {
    if ( service == null || pushDownCondition == null || stepInterface == null ) {
      return false;
    }
    try {
      service.pushDown( pushDownCondition, this, stepInterface );
    } catch ( PushDownOptimizationException e ) {
      return false;
    }
    return true;
  }

  private Condition getPushDownCondition( SQL query ) {
    // Get user query conditions
    Condition whereCondition;
    if ( query.getWhereCondition() != null ) {
      whereCondition = query.getWhereCondition().getCondition();
    } else {
      return null;
    }

    // Attempt to map fields to where clause
    Condition pushDownCondition = mapConditionFields( whereCondition );
    if ( pushDownCondition == null || StringUtils.isBlank( getParameterName() ) ) {
      return null;
    }
    return pushDownCondition;
  }

  protected String setQueryParameter( String query, String parameterValue ) {
    VariableSpace varSpace = new Variables();
    varSpace.setVariable( getParameterName(), parameterValue );
    return varSpace.environmentSubstitute( query );
  }

  @Override public String toString() {
    return Objects.toStringHelper( this )
      .add( "fieldMappings", fieldMappings )
      .add( "parameterName", parameterName )
      .toString();
  }
}
