/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.dataservice.optimization.mongod;

import com.mongodb.QueryBuilder;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.jdbc.ThinUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumeration of supported Mongodb functions mapped to Kettle function constants
 * defined in {@link Condition}.
 * This enumeration includes methods to apply the affirmation
 * and negation of the function to a com.mongodb.QueryBuilder.
 */
public enum MongoFunc {
  LT( Condition.FUNC_SMALLER ) {
    @Override
    public void affirm( QueryBuilder queryBuilder, String attribute, Object value ) {
      queryBuilder
        .and( attribute )
        .lessThan( value );
    }
  },
  LTE( Condition.FUNC_SMALLER_EQUAL ) {
    @Override
    public void affirm( QueryBuilder queryBuilder, String attribute, Object value ) {
      queryBuilder
        .and( attribute )
        .lessThanEquals( value );
    }
  },
  GT( Condition.FUNC_LARGER ) {
    @Override
    public void affirm( QueryBuilder queryBuilder, String attribute, Object value ) {
      queryBuilder
        .and( attribute )
        .greaterThan( value );
    }
  },
  GTE( Condition.FUNC_LARGER_EQUAL ) {
    @Override
    public void affirm( QueryBuilder queryBuilder, String attribute, Object value ) {
      queryBuilder
        .and( attribute )
        .greaterThanEquals( value );
    }
  },
  EQ(  Condition.FUNC_EQUAL ) {
    @Override
    public void affirm( QueryBuilder queryBuilder, String attribute, Object value ) {
      queryBuilder
        .and(  attribute  )
        .is( value );
    }

    @Override
    public void negate( QueryBuilder queryBuilder, String attribute, Object value ) {
      NEQ.affirm( queryBuilder, attribute, value );
    }
  },
  NEQ( Condition.FUNC_NOT_EQUAL ) {
    @Override
    public void affirm( QueryBuilder queryBuilder, String attribute, Object value ) {
      queryBuilder
        .and( attribute )
        .notEquals( value );
    }

    @Override
    public void negate( QueryBuilder queryBuilder, String attribute, Object value ) {
      EQ.affirm( queryBuilder, attribute, value );
    }
  },

  IN( Condition.FUNC_IN_LIST ) {
    @Override
    public void affirm( QueryBuilder queryBuilder, String attribute, Object value ) {
      queryBuilder
        .and( attribute )
        .in( value );
    }

    @Override
    public void negate( QueryBuilder queryBuilder, String attribute, Object value ) {
      queryBuilder
        .and( attribute )
        .notIn( value );
    }
  },

  LIKE( Condition.FUNC_LIKE ) {
    @Override public void affirm( QueryBuilder queryBuilder, String attribute, Object value ) {
      queryBuilder
          .and( attribute )
          .regex( ThinUtil.like( value.toString() ) );
    }
  };

  private final int kettleFuncCode;
  private static final Map<Integer, MongoFunc> kettleToMongo = new HashMap<Integer, MongoFunc>();
  static {
    for ( MongoFunc func : values() ) {
      kettleToMongo.put( func.kettleFuncCode, func );
    }
  }

  MongoFunc( int kettleFuncCode ) {
    this.kettleFuncCode = kettleFuncCode;
  }

  public static MongoFunc getMongoFunc( int kettleFuncCode ) {
    return kettleToMongo.get( kettleFuncCode );
  }

  public abstract void affirm( QueryBuilder queryBuilder, String attribute, Object value );

  public  void negate( QueryBuilder queryBuilder, String attribute, Object value ) {
    affirm( queryBuilder.not(), attribute, value );
  }
}
