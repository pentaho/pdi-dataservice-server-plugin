/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.di.trans.dataservice.optimization.mongod;

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
