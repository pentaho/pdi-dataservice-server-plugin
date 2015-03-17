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

import com.mongodb.util.JSON;
import com.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleSQLException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.sql.SQLCondition;
import org.pentaho.di.core.sql.SQLFields;

import static org.junit.Assert.*;

public class MongodbPredicateTest {

  private RowMeta rowMeta = new RowMeta();
  private ValueMetaResolver resolver;

  @Before
  public void setup() {
    for ( int i = 0; i < 10; i++ ) {
      rowMeta.addValueMeta( new ValueMeta( "sField" + i,
        ValueMetaInterface.TYPE_STRING, 50 ) );
      rowMeta.addValueMeta( new ValueMeta( "iField" + i,
        ValueMetaInterface.TYPE_INTEGER, 7 ) );
    }
    resolver = new ValueMetaResolver( rowMeta );
  }

  @Test
  public void testSimpleConditions() throws KettleException {
    assertJsonEquals( "{\"sField1\":\"foo\"}",
      asFilter( condition( "sField1 = 'foo'" ) ) );
    assertJsonEquals( "{\"$and\":[{\"sField1\":\"foo\"},{\"sField2\":\"bar\"}]}",
      asFilter( condition( "sField1 = 'foo' AND sField2 = 'bar'" ) ) );
    assertJsonEquals( "{\"$or\":[{\"sField1\":\"foo\"},{\"sField2\":\"bar\"}]}",
      asFilter( condition( "sField1 = 'foo' OR sField2 = 'bar'" ) ) );
  }

  @Test
  public void testNotCompoundExpressionThrows() throws KettleException {
    // mongodb only supports $not on atomic conditions
    try {
      asFilter( condition( "NOT ( sField1  IN ('foo','bar','baz') or sField2 = 'baz')" ) );
    } catch ( Exception e ) {
      assertTrue( e instanceof KettleException );
      return;
    }
    fail( "Expected exception." );
  }


  @Test
  public void testInListOfStrings() throws KettleException {
    assertJsonEquals( "{\"sField1\":{\"$in\":[\"foo\",\"bar\",\"baz\"]}}",
      asFilter( condition( "sField1 IN ('foo','bar','baz')" ) ) );
  }

  @Test
  public void testInListOfInts() throws KettleException {
    assertJsonEquals( "{\"iField1\":{\"$in\":[1,2,3]}}",
      asFilter( condition( "iField1 IN (1,2,3)" ) ) );
  }

  @Test
  public void testNotInList() throws KettleException {
    assertJsonEquals( "{\"sField1\":{\"$nin\":[\"foo\" ,\"bar\",\"baz\"]}}",
      asFilter( condition( "NOT (sField1 IN ('foo','bar','baz'))" ) ) );
  }

  @Test
  public void testGT() throws KettleException {
    assertJsonEquals( "{\"iField1\":{\"$gt\":123}}",
      asFilter( condition( "iField1 > 123" ) ) );
  }

  @Test
  public void testMixedComparison() throws KettleException {
    assertJsonEquals( "{\"$and\":[{\"iField1\":{\"$gt\":123}},{\"sField1\":\"foo\"},"
        + "{\"iField2\":{\"$lte\":231}}]}",
      asFilter( condition( "iField1 > 123 AND sField1 = 'foo' AND iField2 <=231" ) ) );
    assertJsonEquals( "{\"$or\":[{\"$and\":[{\"iField1\":{\"$gt\":123}},{\"sField1\":\"foo\"}]},"
        + "{\"iField2\":{\"$ne\":231}}]}",
      asFilter( condition( "(iField1 > 123 AND sField1 = 'foo') OR iField2 <> 231" ) ) );
  }

  @Test
  public void testDeepNesting() throws KettleException {
    assertJsonEquals( "{ \"$or\" : [ { \"iField1\" : 1} , "
        + "{ \"$and\" : [ { \"sField1\" : \"foo\"} , "
        + "{ \"$or\" : [ { \"sField2\" : \"foo\"} , "
        + "{ \"$and\" : [ { \"sField3\" : \"bar\"} , "
        + "{ \"iField2\" : 123}]}]}]}]}",
      asFilter( condition( "iField1 = 1 OR "
        + "(sField1 = 'foo' AND ( sField2 = 'foo' OR "
        + "          (sField3='bar' AND iField2 = 123) ) )" ) ) );
  }

  @Test
  public void testRedundantNesting() throws KettleException {
    assertJsonEquals( "{\"$and\":[{\"$or\":[{\"iField1\":{\"$gt\":123}},{\"sField1\":\"foo\"}]},"
        + "{\"iField2\":{\"$ne\":231}}]}",
      asFilter( condition( "( (iField1 > 123 OR sField1 = 'foo') AND (iField2 <> 231))" ) ) );
  }

  @Test
  public void testNotGTE() throws KettleException {
    assertJsonEquals( "{ \"iField1\" : { \"$not\" : { \"$gte\" : 123}}}",
      asFilter( condition( "NOT (iField1 >= 123)" ) ) );
  }

  @Test
  public void testNotEq() throws KettleException {
    assertJsonEquals( "{\"sField2\":{\"$ne\":\"foo\"}}",
      asFilter( condition( "NOT (sField2 = 'foo')" ) ) );
    assertJsonEquals( "{\"sField2\":{\"$ne\":\"foo\"}}",
      asFilter( condition( "sField2 <> 'foo'" ) ) );
  }

  @Test
  public void testOrNot() throws KettleException {
    assertJsonEquals( "{\"$or\":[{\"sField1\":\"baz\"},{\"sField2\":{\"$ne\":\"foo\"}}]}",
      asFilter( condition( "sField1 = 'baz' OR NOT (sField2 = 'foo')" ) ) );
  }

  @Test
  public void testMatch() throws KettleException {
    assertJsonEquals( "{\"$match\":{\"$or\":[{\"sField1\":\"baz\"},{\"sField2\":{\"$ne\":\"foo\"}}]}}",
      asMatch( condition( "sField1 = 'baz' OR NOT (sField2 = 'foo')" ) ) );
  }

  private String asMatch( Condition condition ) throws KettleException {
    return new MongodbPredicate( condition, resolver ).asMatch();
  }

  private String asFilter( Condition condition ) throws KettleException {
    return new MongodbPredicate( condition, resolver ).asFilterCriteria();
  }

  private Condition condition( String sql ) throws KettleSQLException {
    return new SQLCondition( "service", sql,
      rowMeta, new SQLFields( "service", rowMeta, "sField1, sField2" ) ).getCondition();
  }

  private void assertJsonEquals( String json1, String json2 ) {
    assertEquals(
      JSON.parse( json1 ),
      JSON.parse( json2 ) );
  }
}
