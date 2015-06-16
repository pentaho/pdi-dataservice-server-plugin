/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2015 Pentaho Corporation (Pentaho). All rights reserved.
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

package com.pentaho.di.trans.dataservice.optimization.mongod;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationException;
import com.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaAndData;

import java.util.List;
import java.util.Map;

/**
 * This class translates org.pentaho.di.core.Condition objects to Mongodb
 * predicate expressions.  The string form of this expression can be retrieved
 * as a $match expression suitable for use in an aggregation pipeline, or as
 * filter criteria for use in .find().
 */
public class MongodbPredicate {
  private static final String MATCH = "$match";
  private final ValueMetaResolver valueMetaResolver;
  private Condition condition;
  private Map<String, String> fieldMappings;

  public MongodbPredicate( Condition condition, ValueMetaResolver resolver, Map<String, String> fieldMappings ) {
    this.valueMetaResolver = resolver;
    this.condition = condition;
    this.fieldMappings = fieldMappings;
  }

  public String asMatch() throws PushDownOptimizationException {
    return QueryBuilder.start( MATCH ).is(
      conditionAsDBObject() ).get().toString();
  }

  public String asFilterCriteria() throws PushDownOptimizationException {
    return conditionAsDBObject().toString();
  }

  protected DBObject conditionAsDBObject() throws PushDownOptimizationException {
    return buildMongoCondition( condition, QueryBuilder.start() ).get();
  }

  private QueryBuilder buildMongoCondition( Condition condition, QueryBuilder queryBuilder ) throws PushDownOptimizationException {
    condition = unwrap( condition );
    return condition.isAtomic()
      ? applyAtomicCondition( condition, queryBuilder )
      : applyCompoundCondition( condition, queryBuilder );
  }

  private QueryBuilder applyAtomicCondition( Condition condition, QueryBuilder queryBuilder )
    throws PushDownOptimizationException {
    MongoFunc func = MongoFunc.getMongoFunc( condition.getFunction() );
    String fieldName = getResolvedFieldName( condition.getLeftValuename() );
    Object value = getResolvedValue( condition );
    if ( condition.isNegated() ) {
      func.negate( queryBuilder, fieldName, value );
    } else {
      func.affirm( queryBuilder, fieldName, value );
    }
    return queryBuilder;
  }

  private String getResolvedFieldName( String fieldName ) {

    String resolvedFieldName = fieldMappings.get( fieldName );
    if ( resolvedFieldName != null ) {
      return resolvedFieldName;
    }

    return fieldName;
  }

  /**
   * Gets the Object value associated with the Condition's rightExact.
   * Also handles IN list conversion to typed array.
   */
  private Object getResolvedValue( Condition condition ) throws PushDownOptimizationException {
    final ValueMetaAndData rightExact = condition.getRightExact();
    Object value = rightExact.getValueData();
    int type = rightExact.getValueMeta().getType();
    String fieldName = condition.getLeftValuename();
    if ( condition.getFunction() == Condition.FUNC_IN_LIST ) {
      return valueMetaResolver.inListToTypedObjectArray( fieldName, (String) value );
    }
    return valueMetaResolver.getTypedValue( fieldName, type, value );
  }

  private QueryBuilder applyCompoundCondition( Condition condition, QueryBuilder queryBuilder )
    throws PushDownOptimizationException {
    getMongoOp( condition )
      .apply( queryBuilder,
        conditionListToDBObjectArray( condition.getChildren() ) );
    return queryBuilder;
  }

  private MongoOp getMongoOp( Condition condition )
    throws PushDownOptimizationException {
    validateConditionForOpDetermination( condition );

    final Condition firstChild = condition.getChildren().get( 1 );
    MongoOp op = MongoOp
      .getMongoOp( firstChild.getOperator() );
    if ( op == null ) {
      throw new PushDownOptimizationException( "Unsupported operator:  " + firstChild.getOperatorDesc() );
    }
    return op;
  }

  private void validateConditionForOpDetermination( Condition condition )
    throws PushDownOptimizationException {
    if ( condition.getChildren().size() <= 1 ) {
      throw new PushDownOptimizationException( "At least 2 children are required to determine connecting operator." );
    }
    if ( condition.isNegated() ) {
      throw new PushDownOptimizationException( "Negated non-atomic conditions can't be converted to BSON" );
    }
  }

  private DBObject[] conditionListToDBObjectArray( List<Condition> conditions )
    throws PushDownOptimizationException {
    BasicDBList basicDbList = new BasicDBList();
    for ( Condition condition : conditions ) {
      QueryBuilder childContainer = QueryBuilder.start();
      buildMongoCondition( condition, childContainer );
      basicDbList.add( childContainer.get() );
    }
    return basicDbList.toArray( new DBObject[basicDbList.size()] );
  }

  /**
   * Strips off redundent condition nesting.
   */
  private Condition unwrap( Condition condition ) {
    return condition.getChildren().size() == 1 && !condition.isNegated()
      ? unwrap( condition.getChildren().get( 0 ) ) : condition;
  }
}

