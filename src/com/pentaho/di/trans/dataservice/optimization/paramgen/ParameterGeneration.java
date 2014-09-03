/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */
package com.pentaho.di.trans.dataservice.optimization.paramgen;

import com.pentaho.di.trans.dataservice.DataServiceExecutor;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationException;
import com.pentaho.di.trans.dataservice.optimization.PushDownType;
import com.pentaho.di.trans.dataservice.optimization.SourceTargetFields;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.steps.tableinput.TableInput;
import org.pentaho.di.trans.steps.tableinput.TableInputData;
import org.pentaho.metastore.persist.MetaStoreAttribute;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author nhudak
 */
public class ParameterGeneration implements PushDownType {

  public static final String TYPE_NAME = "Parameter Generation";

  // Default to WHERE clause generation
  @MetaStoreAttribute
  private OptimizationForm form = OptimizationForm.WHERE_CLAUSE;

  @MetaStoreAttribute
  private List<SourceTargetFields> fieldMappings = new ArrayList<SourceTargetFields>();

  @MetaStoreAttribute
  private String parameterName;

  public String getParameterName() {
    return parameterName == null ? "" : parameterName;
  }

  public void setParameterName( String parameterName ) {
    this.parameterName = parameterName;
  }

  @Override public String getTypeName() {
    return TYPE_NAME;
  }

  public OptimizationForm getForm() {
    return form;
  }

  public void setForm( OptimizationForm form ) {
    this.form = form;
  }

  @Override public String getFormName() {
    return form.getFormName();
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

  @Override public boolean activate( DataServiceExecutor executor, Trans trans, StepInterface stepInterface, SQL query ) {
    // Get step's database
    Database db;
    if ( stepInterface instanceof TableInput ) {
      TableInput tableInput = (TableInput) stepInterface;
      TableInputData stepDataInterface = (TableInputData) tableInput.getStepDataInterface();
      db = stepDataInterface.db;
    } else {
      return false;
    }
    // Get user query conditions
    List<Condition> conditions;
    if ( query.getWhereCondition() != null ) {
      Condition whereCondition = query.getWhereCondition().getCondition();
      if ( whereCondition.getChildren().isEmpty() ) {
        conditions = Collections.singletonList( whereCondition );
      } else {
        conditions = whereCondition.getChildren();
      }
    } else {
      return false;
    }

    String sqlFragment;
    try {
      sqlFragment = buildSqlFragment( db, conditions );
    } catch ( PushDownOptimizationException e ) {
      return false;
    }

    if ( StringUtils.isNotBlank( getParameterName() ) && StringUtils.isNotBlank( sqlFragment ) ) {
      stepInterface.setVariable( getParameterName(), sqlFragment );
    }

    return true;
  }

  protected String buildSqlFragment( Database db, List<Condition> conditions ) throws PushDownOptimizationException {
    StringBuilder builder = new StringBuilder();
    boolean first = true;
    for ( Condition condition : conditions ) {
      if ( !first && condition.getOperator() != Condition.OPERATOR_AND ) {
        // Query can not be pushed down because it is not simply joined
        throw new PushDownOptimizationException( "Parameter Generation does not support operators other than AND" );
      }
      if ( conditionSupported( condition, db ) ) {
        String mappedCondition = convertCondition( condition, db );
        if ( mappedCondition != null ) {
          // Condition supported and converted to push down condition
          builder.append( first ? form.getPrefix() + " " : " AND " );
          builder.append( mappedCondition );
          first = false;
        }
      }
    }
    return builder.toString();
  }

  protected boolean conditionSupported( Condition condition, Database db ) throws PushDownOptimizationException {
    // TODO need better logic to determine supported operations, simplified here to only support '='
    return condition.getFunction() == Condition.FUNC_EQUAL && condition.getChildren().isEmpty();
  }

  protected String convertCondition( Condition condition, Database db ) throws PushDownOptimizationException {
    String target = null;
    for ( SourceTargetFields fieldMapping : fieldMappings ) {
      if ( fieldMapping.getSourceFieldName().equals( condition.getLeftValuename() ) ) {
        target = fieldMapping.getTargetFieldName();
        break;
      }
    }

    // Ensure target is mapped
    if ( target == null ) {
      // Return Null: Skip this condition
      return null;
    }

    // TODO need better conversion logic, using only simple field mapping here, defer to DB?
    String value = condition.getRightExactString();
    if ( condition.getRightExact().getValueMeta().isString() ) {
      // Wrap string constant in quotes
      value = "'" + value + "'";
    }
    return String.format( "%s %s %s", target, condition.getFunctionDesc(), value );
  }
}
