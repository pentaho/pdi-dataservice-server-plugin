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

import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationException;
import com.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaAndData;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.steps.tableinput.TableInput;
import org.pentaho.di.trans.steps.tableinput.TableInputData;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @author nhudak
 */
public class TableInputParameterGeneration implements ParameterGenerationService {

  private final ValueMetaResolver valueMetaResolver;

  public TableInputParameterGeneration( ValueMetaResolver resolver ) {
    valueMetaResolver = resolver;
  }

  @Override public String getParameterDefault() {
    return "1=1";
  }

  @Override
  public void pushDown( Condition condition, ParameterGeneration parameterGeneration, StepInterface stepInterface ) throws PushDownOptimizationException {
    TableInput tableInput;
    if ( stepInterface instanceof TableInput ) {
      tableInput = (TableInput) stepInterface;
    } else {
      throw new PushDownOptimizationException( "Unable to push down to push down to type " + stepInterface.getClass() );
    }

    TableInputData stepDataInterface = (TableInputData) tableInput.getStepDataInterface();
    DatabaseWrapper db;
    if ( stepDataInterface.db instanceof DatabaseWrapper ) {
      // Use existing wrapper if available
      db = (DatabaseWrapper) stepDataInterface.db;
    } else {
      // Create a new wrapper
      db = new DatabaseWrapper( stepDataInterface.db );
    }

    // Verify database connection
    try {
      db.connect();
    } catch ( KettleDatabaseException e ) {
      throw new PushDownOptimizationException( "Failed to verify database connection", e );
    }

    StringBuilder sqlFragment = new StringBuilder();
    RowMeta paramsMeta = new RowMeta();
    List<Object> params = new LinkedList<Object>();

    convertCondition( condition, sqlFragment, paramsMeta, params );

    // Save conversion results for injection at runtime
    String fragmentId = db.createRuntimePushDown( sqlFragment.toString(), paramsMeta, params );

    // Set variable to fragment ID
    stepInterface.setVariable( parameterGeneration.getParameterName(), fragmentId );
    stepDataInterface.db = db;
  }


  protected void convertCondition( Condition condition, StringBuilder builder, RowMeta paramsMeta, List<Object> params )
    throws PushDownOptimizationException {
    // Condition is composite: Recursively add children
    if ( condition.isComposite() ) {
      builder.append( "( " );
      for ( Condition child : condition.getChildren() ) {
        if ( child.getOperator() == Condition.OPERATOR_AND ) {
          builder.append( " AND " );
        } else if ( child.getOperator() == Condition.OPERATOR_OR ) {
          builder.append( " OR " );
        }
        convertCondition( child, builder, paramsMeta, params );
      }
      builder.append( " )" );
    } else {
      builder.append( convertAtomicCondition( condition, paramsMeta, params ) );
    }
  }

  protected String convertAtomicCondition( Condition condition, RowMeta paramsMeta, List<Object> params )
    throws PushDownOptimizationException {
    String value = condition.getRightExactString();
    String function = condition.getFunctionDesc();
    String placeholder = "?";

    switch ( condition.getFunction() ) {
      case Condition.FUNC_REGEXP:
      case Condition.FUNC_CONTAINS:
      case Condition.FUNC_STARTS_WITH:
      case Condition.FUNC_ENDS_WITH:
      case Condition.FUNC_TRUE:
        throw new PushDownOptimizationException( condition.getFunctionDesc()
          + " is not supported for push down." );
      case Condition.FUNC_NOT_NULL:
      case Condition.FUNC_NULL:
        placeholder = "";
        break;
      case Condition.FUNC_IN_LIST:
        Object[] inList = getInListArray( condition );
        ValueMetaInterface inListValueMeta = getResolvedValueMeta( condition );
        for ( int i = 0; i < inList.length; i++ ) {
          paramsMeta.addValueMeta( inListValueMeta );
          params.add( inList [ i ] );
        }
        placeholder = String.format( "(%s)", StringUtils.join( Collections.nCopies( inList.length, "?" ).iterator(), "," ) );
        function = " IN ";
        break;
      default:
        if ( value == null ) {
          throw new PushDownOptimizationException( "Condition value can not be null: " + condition );
        }
        paramsMeta.addValueMeta( getResolvedValueMeta( condition ) );
        params.add( getResolvedValue( condition ) );
        break;
    }
    return String.format( "\"%s\" %s %s", getFieldName( condition ), function, placeholder );
  }

  private Object[] getInListArray( Condition condition ) throws PushDownOptimizationException {
    String value = condition.getRightExactString();
    return valueMetaResolver.inListToTypedObjectArray(
      getFieldName( condition ), value );
  }

  private Object getResolvedValue( Condition condition ) throws PushDownOptimizationException {
    ValueMetaAndData metaAndData = condition.getRightExact();
    String fieldName = getFieldName( condition );
    return valueMetaResolver.getTypedValue(
      fieldName, metaAndData.getValueMeta().getType(),
      metaAndData.getValueData() );
  }

  private ValueMetaInterface getResolvedValueMeta( Condition condition ) throws PushDownOptimizationException {
    return valueMetaResolver.getValueMeta( getFieldName( condition ) );
  }

  private String getFieldName( Condition condition ) {
    assert condition.isAtomic();
    return condition.getLeftValuename();
  }


}
