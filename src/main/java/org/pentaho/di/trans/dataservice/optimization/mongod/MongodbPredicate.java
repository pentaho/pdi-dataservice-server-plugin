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

package org.pentaho.di.trans.dataservice.optimization.mongod;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationException;
import org.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
import org.pentaho.di.core.Condition;
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

