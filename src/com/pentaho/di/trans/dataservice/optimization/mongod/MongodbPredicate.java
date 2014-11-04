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

package com.pentaho.di.trans.dataservice.optimization.mongod;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.util.List;

/**
 * This class translates org.pentaho.di.core.Condition objects to Mongodb
 * predicate expressions.  The string form of this expression can be retrieved
 * as a $match expression suitable for use in an aggregation pipeline, or as
 * filter criteria for use in .find().
 */
public class MongodbPredicate {
  private static final String MATCH = "$match";
  private Condition condition;

  public MongodbPredicate( Condition condition ) {
    this.condition = condition;
  }

  public String asMatch() throws KettleException {
    return QueryBuilder.start( MATCH ).is(
      conditionAsDBObject() ).get().toString();
  }

  public String asFilterCriteria() throws KettleException {
    return conditionAsDBObject().toString();
  }

  protected DBObject conditionAsDBObject() throws KettleException {
    return buildMongoCondition( condition, QueryBuilder.start() ).get();
  }

  private QueryBuilder buildMongoCondition( Condition condition, QueryBuilder queryBuilder ) throws KettleException {
    condition = unwrap( condition );
    return condition.isAtomic()
      ? applyAtomicCondition( condition, queryBuilder )
      : applyCompoundCondition( condition, queryBuilder );
  }

  private QueryBuilder applyAtomicCondition( Condition condition, QueryBuilder queryBuilder ) throws KettleValueException {
    MongoFunc func = MongoFunc.getMongoFunc( condition.getFunction() );
    String fieldName = condition.getLeftValuename();
    Object value = getRightExactValue( condition );
    if ( condition.isNegated() ) {
      func.negate( queryBuilder, fieldName, value );
    } else {
      func.affirm( queryBuilder, fieldName,  value );
    }
    return queryBuilder;
  }

  /**
   * Gets the Object value associated with the Condition's rightExact,
   * applying special handling for IN list conversion to String[]
   */
  private Object getRightExactValue( Condition condition ) throws KettleValueException {
    Object value = condition.getRightExact().getValueData();
    if ( condition.getFunction() == condition.FUNC_IN_LIST ) {
      ValueMetaInterface valueMeta = condition.getRightExact().getValueMeta();
      return Const.splitString( valueMeta.getString( value ), ';', true );
    }
    return value;
  }

  private QueryBuilder applyCompoundCondition( Condition condition, QueryBuilder queryBuilder )
    throws KettleException {
    getMongoOp( condition )
      .apply( queryBuilder,
        conditionListToDBObjectArray( condition.getChildren() ) );
    return queryBuilder;
  }

  private MongoOp getMongoOp( Condition condition ) throws KettleException {
    validateConditionForOpDetermination( condition );

    final Condition firstChild = condition.getChildren().get( 1 );
    MongoOp op = MongoOp
      .getMongoOp( firstChild.getOperator() );
    if ( op == null ) {
      throw new KettleException( "Unsupported operator:  " + firstChild.getOperatorDesc() );
    }
    return op;
  }

  private void validateConditionForOpDetermination( Condition condition ) throws KettleException {
    if ( condition.getChildren().size() <= 1 ) {
      throw new KettleException( "At least 2 children are required to determine connecting operator." );
    }
    if ( condition.isNegated() ) {
      throw new KettleException( "Negated non-atomic conditions can't be converted to BSON" );
    }
  }

  private DBObject[] conditionListToDBObjectArray( List<Condition> conditions )
    throws KettleException {
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

