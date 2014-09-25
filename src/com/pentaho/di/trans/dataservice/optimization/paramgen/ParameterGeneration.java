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
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.steps.tableinput.TableInput;
import org.pentaho.di.trans.steps.tableinput.TableInputData;
import org.pentaho.metastore.persist.MetaStoreAttribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

  protected Condition mapConditionFields( Condition condition ) {
    Condition clone = (Condition) condition.clone();
    HashMap<String, SourceTargetFields> sourceMap;
    sourceMap = new HashMap<String, SourceTargetFields>();
    for ( SourceTargetFields fieldMapping : fieldMappings ) {
      sourceMap.put( fieldMapping.getSourceFieldName(), fieldMapping );
    }

    Map<String, SourceTargetFields> sourceTargetFieldsMap = sourceMap;
    if ( applyMapping( clone, sourceTargetFieldsMap ) ) {
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
      boolean requireAll = false;
      for ( Condition child : children ) {
        if ( child.getOperator() == Condition.OPERATOR_OR ) {
          requireAll = true;
          break;
        }
      }

      // Map each child
      for ( Iterator<Condition> i = children.iterator(); i.hasNext();) {
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
    Condition whereCondition;
    if ( query.getWhereCondition() != null ) {
      whereCondition = query.getWhereCondition().getCondition();
    } else {
      return false;
    }

    // Attempt to map fields to where clause
    Condition pushDownCondition = mapConditionFields( whereCondition );
    if ( pushDownCondition == null ) {
      return false;
    }

    // TODO: Defer sql generation to database meta interface, new plugin? service?
    String sqlFragment;
    try {
      sqlFragment = convertCondition( pushDownCondition, db );
    } catch ( PushDownOptimizationException e ) {
      return false;
    }

    if ( StringUtils.isNotBlank( getParameterName() ) && StringUtils.isNotBlank( sqlFragment ) ) {
      stepInterface.setVariable( getParameterName(), form.getPrefix() + " " + sqlFragment );
    }

    return true;
  }

  protected String convertCondition( Condition condition, Database db ) throws PushDownOptimizationException {
    StringBuilder output = new StringBuilder();
    if ( condition.isComposite() ) {
      output.append( " ( " );
      for ( Condition child : condition.getChildren() ) {
        if ( child.getOperator() == Condition.OPERATOR_AND ) {
          output.append( " AND " );
        } else if ( child.getOperator() == Condition.OPERATOR_OR ) {
          output.append( " OR " );
        }
        output.append( convertCondition( child, db ) );
      }
      output.append( " ) " );
    } else {
      output.append( convertAtomicCondition( condition, db ) );
    }
    return output.toString();
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
        assert value == null;
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
        assert value != null;
        if ( condition.getRightExact().getValueMeta().isString() ) {
          // Wrap string constant in quotes
          value = db.getDatabaseMeta().quoteSQLString( value );
        }
        break;
    }
    return String.format( "\"%s\" %s %s", condition.getLeftValuename(), function, value );
  }

}
