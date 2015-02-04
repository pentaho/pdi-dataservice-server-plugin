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

import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import org.pentaho.di.core.Condition;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumeration of supported Mongodb operations mapped to Kettle operation constants
 * defined in {@link Condition}.
 * The enum includes a method to apply the operation to a DBObject array.  For example,
 * given a DBObject array
 *
 *     foobar = [ { field1 : 'foo'}, {field2 : 'bar'} ],
 *
 *     AND.apply( queryBuilder, foobar )
 *
 * would result in a QueryBuilder with the subexpression
 *
 *   { $and : [ { field1 : 'foo'}, {field2 : 'bar'} ] }
 */
public enum MongoOp {
  AND( Condition.OPERATOR_AND ) {
    @Override
    public void apply( QueryBuilder queryBuilder, DBObject... ands ) {
      queryBuilder.and( ands );
    }
  },
  OR( Condition.OPERATOR_OR ) {
    @Override
    public void apply( QueryBuilder queryBuilder, DBObject... ors ) {
      queryBuilder.or( ors );
    }
  };

  private final int kettleOpCode;

  private static final Map<Integer, MongoOp> kettleToMongo = new HashMap<Integer, MongoOp>();
  static {
    for ( MongoOp op : values() ) {
      kettleToMongo.put( op.kettleOpCode, op );
    }
  }

  public static MongoOp getMongoOp( int kettleOpCode ) {
    return kettleToMongo.get( kettleOpCode );
  }

  MongoOp( int kettleOperator ) {
    kettleOpCode = kettleOperator;
  }

  public abstract void apply( QueryBuilder queryBuilder, DBObject... objects );
}
