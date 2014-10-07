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
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.steps.tableinput.TableInput;
import org.pentaho.di.trans.steps.tableinput.TableInputData;

/**
 * @author nhudak
 */
public class TableInputParameterGeneration implements ParameterGenerationService {

  public TableInputParameterGeneration() {
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
    Database db = stepDataInterface.db;
    // Verify database connection
    try {
      db.connect();
    } catch ( KettleDatabaseException e ) {
      throw new PushDownOptimizationException( "Failed to verify database connection", e );
    }

    StringBuilder sqlFragment = new StringBuilder();

    convertCondition( condition, sqlFragment, db );

    // Set variable to sql fragment
    stepInterface.setVariable( parameterGeneration.getParameterName(), sqlFragment.toString() );
  }

  protected void convertCondition( Condition condition, StringBuilder builder, Database db )
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
        convertCondition( child, builder, db );
      }
      builder.append( " )" );
    } else {
      builder.append( convertAtomicCondition( condition, db ) );
    }
  }

  protected String convertAtomicCondition( Condition condition, Database db ) throws PushDownOptimizationException {
    String value = condition.getRightExactString();
    String function = condition.getFunctionDesc();

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
        value = "";
        break;
      case Condition.FUNC_IN_LIST:
        ValueMetaInterface valueMeta = condition.getRightExact().getValueMeta();
        String[] inList;
        try {
          inList = Const.splitString( valueMeta.getString( value ), ';', true );
        } catch ( KettleValueException e ) {
          throw new PushDownOptimizationException( "Failed to convert condition for push down optimization", e );
        }
        for ( int i = 0; i < inList.length; i++ ) {
          inList[ i ] = inList[ i ] == null ? null
            : db.getDatabaseMeta().quoteSQLString( inList[ i ].replace( "\\", "" ) );
        }
        value = String.format( "(%s)", StringUtils.join( inList, "," ) );
        function = " IN ";
        break;
      default:
        if ( value == null ) {
          throw new PushDownOptimizationException( "Condition value can not be null: " + condition );
        }
        if ( condition.getRightExact().getValueMeta().isString() ) {
          // Wrap string constant in quotes
          value = db.getDatabaseMeta().quoteSQLString( value );
        }
        break;
    }
    return String.format( "\"%s\" %s %s", condition.getLeftValuename(), function, value );
  }
}
