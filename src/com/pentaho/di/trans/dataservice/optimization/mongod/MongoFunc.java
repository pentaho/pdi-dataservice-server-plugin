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

import com.mongodb.QueryBuilder;
import org.pentaho.di.core.Condition;

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
