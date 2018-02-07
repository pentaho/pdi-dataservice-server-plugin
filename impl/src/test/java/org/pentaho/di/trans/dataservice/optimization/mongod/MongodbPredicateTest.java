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

import com.mongodb.util.JSON;
import org.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
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

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MongodbPredicateTest {

  private RowMeta rowMeta = new RowMeta();
  private ValueMetaResolver resolver;
  private Map<String, String> fieldMappings;

  @Before
  public void setup() {
    for ( int i = 0; i < 10; i++ ) {
      rowMeta.addValueMeta( new ValueMeta( "sField" + i,
        ValueMetaInterface.TYPE_STRING, 50 ) );
      rowMeta.addValueMeta( new ValueMeta( "iField" + i,
        ValueMetaInterface.TYPE_INTEGER, 7 ) );
    }
    resolver = new ValueMetaResolver( rowMeta );
    fieldMappings = new HashMap<String, String>();
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

  @Test public void testLike() throws KettleException {
    assertJsonEquals( "{ \"$match\" : { \"sField1\" : { \"$regex\" : \".*?foo.*?\" , \"$options\" : \"is\"}}}",
        asMatch( condition( "sField1 LIKE '%foo%'" ) ) );
  }

  private String asMatch( Condition condition ) throws KettleException {
    return new MongodbPredicate( condition, resolver, fieldMappings ).asMatch();
  }

  private String asFilter( Condition condition ) throws KettleException {
    return new MongodbPredicate( condition, resolver, fieldMappings ).asFilterCriteria();
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
