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
