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

import org.pentaho.di.trans.dataservice.optimization.OptimizationImpactInfo;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationException;
import org.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaAndData;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.steps.tableinput.TableInput;
import org.pentaho.di.trans.steps.tableinput.TableInputData;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @author nhudak
 */
public class TableInputParameterGeneration implements ParameterGenerationService {

  private final ValueMetaResolver valueMetaResolver;
  protected DatabaseMeta dbMeta;

  public TableInputParameterGeneration( ValueMetaResolver resolver ) {
    valueMetaResolver = resolver;
  }

  @Override public String getParameterDefault() {
    return "1=1";
  }

  @Override
  public OptimizationImpactInfo preview( Condition pushDownCondition, ParameterGeneration parameterGeneration, StepInterface stepInterface ) {
    OptimizationImpactInfo optimizationInfo = new OptimizationImpactInfo( stepInterface.getStepname() );

    try {
      final String sql = getSQL( stepInterface );
      optimizationInfo.setQueryBeforeOptimization( sql );

      if ( pushDownCondition == null ) {
        optimizationInfo.setModified( false );
        return optimizationInfo;
      }

      DatabaseWrapper db = getDatabaseWrapper( stepInterface );

      StringBuilder sqlFragment = new StringBuilder();
      RowMeta paramsMeta = new RowMeta();
      List<Object> params = new LinkedList<Object>();

      dbMeta = db.getDatabaseMeta();

      convertCondition( pushDownCondition, sqlFragment, paramsMeta, params );
      String fragmentId = db.createRuntimePushDown( sqlFragment.toString(), paramsMeta, params, getParameterDefault() );

      final String modifiedSql = db.injectRuntime(
        db.pushDownMap,
        parameterGeneration.setQueryParameter( sql, fragmentId ),
        //setVariableInSql( sql, parameterGeneration.getParameterName(), fragmentId ),
        new RowMeta(), new LinkedList<Object>() );

      optimizationInfo.setQueryAfterOptimization( db.parameterizedQueryToString( modifiedSql, params ) );
      optimizationInfo.setModified( true );

    } catch ( PushDownOptimizationException e ) {
      optimizationInfo.setModified( false );
      optimizationInfo.setErrorMsg( e.getMessage() );
    }
    return optimizationInfo;
  }

  @Override
  public void pushDown( Condition condition, ParameterGeneration parameterGeneration, StepInterface stepInterface ) throws PushDownOptimizationException {
    DatabaseWrapper db = getDatabaseWrapper( stepInterface );
    verifyDbConnection( db );

    dbMeta = db.getDatabaseMeta();

    StringBuilder sqlFragment = new StringBuilder();
    RowMeta paramsMeta = new RowMeta();
    List<Object> params = new LinkedList<Object>();

    convertCondition( condition, sqlFragment, paramsMeta, params );

    // Save conversion results for injection at runtime
    String fragmentId = db.createRuntimePushDown( sqlFragment.toString(), paramsMeta, params, getParameterDefault() );

    // Set variable to fragment ID
    stepInterface.setVariable( parameterGeneration.getParameterName(), fragmentId );

    TableInputData tableInput = getTableInputData( stepInterface );
    tableInput.db = db;
  }

  private void verifyDbConnection( DatabaseWrapper db ) throws PushDownOptimizationException {
    try {
      db.connect();
    } catch ( KettleDatabaseException e ) {
      throw new PushDownOptimizationException( "Failed to verify database connection", e );
    }
  }

  private DatabaseWrapper getDatabaseWrapper( StepInterface stepInterface ) throws PushDownOptimizationException {
    TableInputData stepDataInterface = getTableInputData( stepInterface );
    DatabaseWrapper db;
    if ( stepDataInterface.db instanceof DatabaseWrapper ) {
      // Use existing wrapper if available
      db = (DatabaseWrapper) stepDataInterface.db;
    } else {
      // Create a new wrapper
      db = new DatabaseWrapper( stepDataInterface.db );
    }
    return db;
  }

  private TableInputData getTableInputData( StepInterface stepInterface ) throws PushDownOptimizationException {
    TableInput tableInput;
    if ( stepInterface instanceof TableInput ) {
      tableInput = (TableInput) stepInterface;
    } else {
      throw new PushDownOptimizationException( "Unable to push down to push down to type " + stepInterface.getClass() );
    }
    return (TableInputData) tableInput.getStepDataInterface();
  }

  private String getSQL( StepInterface stepInterface ) throws PushDownOptimizationException {
    TableInput tableInput;
    if ( stepInterface instanceof TableInput ) {
      tableInput = (TableInput) stepInterface;
    } else {
      throw new PushDownOptimizationException( "Unable to push down to push down to type " + stepInterface.getClass() );
    }
    final TableInputMeta tableInputMeta = (TableInputMeta) tableInput.getStepMeta().getStepMetaInterface();
    return tableInputMeta.getSQL();
  }

  protected void convertCondition( Condition condition, StringBuilder builder, RowMeta paramsMeta, List<Object> params )
    throws PushDownOptimizationException {
    // Condition is composite: Recursively add children
    if ( condition.isNegated() ) {
      builder.append( "NOT " );
    }
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

    return String.format( "%s %s %s", getQuotedFieldName( condition ), function, placeholder );
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

  private String getQuotedFieldName( Condition condition ) {
    String fieldName = dbMeta.quoteField( getFieldName( condition ) );
    return fieldName;
  }

}
