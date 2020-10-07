/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.samplerows.SampleRowsMeta;
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;

import java.util.Arrays;
import java.util.Calendar;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.pentaho.di.core.row.ValueMetaInterface.TYPE_NONE;
import static org.pentaho.di.core.row.ValueMetaInterface.TYPE_NUMBER;

public class SqlTransGeneratorTest {

  @Before
  public void before() throws KettlePluginException {
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init();
  }

  @Test
  public void testGenTransUsingSqlWithAggAndAliases() throws KettleException {
    SQL sql = new SQL( "SELECT \"FACT\".\"mth\" AS \"COL0\",AVG(\"FACT\".\"bmi\") AS \"COL1\","
        + "\"FACT\".\"gender\" AS \"COL2\" FROM \"FACT\" GROUP BY  \"FACT\".\"mth\",\"FACT\".\"gender\"" );
    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "mth" ) );
    rowMeta.addValueMeta( new ValueMetaInteger( "bmi" ) );
    rowMeta.addValueMeta( new ValueMetaString( "gender" ) );
    sql.parse( rowMeta );

    SqlTransGenerator generator = new SqlTransGenerator( sql, 0 );

    SelectValuesMeta selectValuesMeta = getSelectStepValuesMeta( generator.generateTransMeta() );
    assertThat( selectValuesMeta.getSelectName(), equalTo( new String[] { "mth", "COL1", "gender" } ) );
    assertThat( selectValuesMeta.getSelectRename(), equalTo( new String[] { "COL0", null, "COL2" } ) );
  }

  @Test
  public void testGenTransUsingSqlWithGroupByOrderByNoAlias() throws KettleException {
    SQL sql = new SQL( "SELECT foo, bar, baz FROM table GROUP BY foo, bar, baz ORDER BY foo" );
    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "foo" ) );
    rowMeta.addValueMeta( new ValueMetaInteger( "bar" ) );
    rowMeta.addValueMeta( new ValueMetaString( "baz" ) );
    sql.parse( rowMeta );

    SqlTransGenerator generator = new SqlTransGenerator( sql, 0 );

    SelectValuesMeta selectValuesMeta = getSelectStepValuesMeta( generator.generateTransMeta() );
    assertThat( selectValuesMeta.getSelectName(), equalTo( new String[] { "foo", "bar", "baz" } ) );
    assertThat( selectValuesMeta.getSelectRename(), equalTo( new String[] { null, null, null } ) );
  }

  @Test
  public void testGenTransAllAggsWithAliases() throws KettleException {
    SQL sql = new SQL( "SELECT sum(foo) as c1, avg(bar) as c2, max(baz) as c3 FROM table" );
    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "foo" ) );
    rowMeta.addValueMeta( new ValueMetaInteger( "bar" ) );
    rowMeta.addValueMeta( new ValueMetaString( "baz" ) );
    sql.parse( rowMeta );

    SqlTransGenerator generator = new SqlTransGenerator( sql, 0 );

    SelectValuesMeta selectValuesMeta = getSelectStepValuesMeta( generator.generateTransMeta() );
    assertThat( selectValuesMeta.getSelectName(), equalTo( new String[] { "c1", "c2", "c3" } ) );
    assertThat( selectValuesMeta.getSelectRename(), equalTo( new String[] { null, null, null } ) );
  }

  @Test
  public void testTypeHandlingWithAvg() throws KettleException {
    SQL sql = new SQL( "SELECT avg( intVal ), avg( numericVal ), avg( bigNumber)  FROM table" );
    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaInteger( "intVal" ) );
    rowMeta.addValueMeta( new ValueMetaNumber( "numericVal" ) );
    rowMeta.addValueMeta( new ValueMetaBigNumber( "bigNumber" ) );
    sql.parse( rowMeta );

    SqlTransGenerator generator = new SqlTransGenerator( sql, 0 );
    SelectValuesMeta meta =
      (SelectValuesMeta) getStepByName( generator.generateTransMeta(), "Set Conversion" );
    assertThat( meta.getMeta().length, is( 3 ) );
    // Integer should be converted to TYPE_NUMBER
    assertThat( meta.getMeta()[0].getType(), is( TYPE_NUMBER ) );

    // Other types should be left alone ( SelectMetadataChange.getType remains TYPE_NONE )
    assertThat( meta.getMeta()[1].getType(), is( TYPE_NONE ) );
    assertThat( meta.getMeta()[2].getType(), is( TYPE_NONE ) );
  }

  @Test
  public void testGenTransAllAggsNoAliases() throws KettleException {
    SQL sql = new SQL( "SELECT sum(foo), avg(bar), max(baz) FROM table" );
    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "foo" ) );
    rowMeta.addValueMeta( new ValueMetaInteger( "bar" ) );
    rowMeta.addValueMeta( new ValueMetaString( "baz" ) );
    sql.parse( rowMeta );

    SqlTransGenerator generator = new SqlTransGenerator( sql, 0 );

    SelectValuesMeta selectValuesMeta = getSelectStepValuesMeta( generator.generateTransMeta() );
    assertThat( selectValuesMeta.getSelectName(), equalTo( new String[] { "foo", "bar", "baz" } ) );
    assertThat( selectValuesMeta.getSelectRename(), equalTo( new String[] { null, null, null } ) );
  }

  @Test
  public void testGenTransSelectStar() throws KettleException {
    SQL sql = new SQL( "SELECT * FROM table" );
    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "foo" ) );
    sql.parse( rowMeta );

    SqlTransGenerator generator = new SqlTransGenerator( sql, 0 );

    SelectValuesMeta selectValuesMeta = getSelectStepValuesMeta( generator.generateTransMeta() );
    assertThat( selectValuesMeta.getSelectName(), equalTo( new String[] { "foo" } ) );
    assertThat( selectValuesMeta.getSelectRename(), equalTo( new String[] { null } ) );
  }

  private SelectValuesMeta getSelectStepValuesMeta( TransMeta transMeta ) {
    return (SelectValuesMeta) getStepByName( transMeta, "Select values" );
  }

  private StepMetaInterface getStepByName( TransMeta transMeta, String stepName ) {
    int selectValuesIndex = Arrays.asList( transMeta.getStepNames() ).indexOf( stepName );
    if ( selectValuesIndex < 0 ) {
      fail( "Expected a step named '" + stepName + "'" );
    }
    return  transMeta.getStep( selectValuesIndex ).getStepMetaInterface();
  }

  @Test
  public void testLimitClause() throws KettleException {
    SQL sql = new SQL( "SELECT * FROM table LIMIT 1" );
    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "foo" ) );
    sql.parse( rowMeta );

    SqlTransGenerator generator = new SqlTransGenerator( sql, 0 );

    SelectValuesMeta selectValuesMeta = getSelectStepValuesMeta( generator.generateTransMeta() );
    assertThat( selectValuesMeta.getSelectName(), equalTo( new String[] { "foo" } ) );
    assertThat( selectValuesMeta.getSelectRename(), equalTo( new String[] { null } ) );
    assertThat( sql.getLimitClause(), notNullValue() );
  }

  @Test
  public void testGenerateIifStep() throws KettleException {
    SQL sql = new SQL( "SELECT IIF(10 > 1, 'TRUE', 'FALSE') FROM table" );
    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaInteger( "foo" ) );
    sql.parse( rowMeta );

    SqlTransGenerator generator = new SqlTransGenerator( sql, 0 );

    SelectValuesMeta selectValuesMeta = getSelectStepValuesMeta( generator.generateTransMeta() );
    assertThat( selectValuesMeta.getSelectName(), equalTo( new String[] { "IIF(10 > 1, 'TRUE', 'FALSE')" } ) );
    assertThat( selectValuesMeta.getSelectRename(), equalTo( new String[] { null } ) );
  }

  @Test
  public void testGenerateConstStep() throws KettleException {
    SQL sql = new SQL( "SELECT 'FOO' as foo FROM table" );
    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaInteger( "foo" ) );
    sql.parse( rowMeta );

    SqlTransGenerator generator = new SqlTransGenerator( sql, 0 );

    SelectValuesMeta selectValuesMeta = getSelectStepValuesMeta( generator.generateTransMeta() );
    assertThat( selectValuesMeta.getSelectName(), equalTo( new String[] { "'FOO'" } ) );
    assertThat( selectValuesMeta.getSelectRename(), equalTo( new String[] { "foo" } ) );
  }

  @Test
  public void testFilterStep() throws KettleException {
    SQL sql = new SQL( "SELECT * FROM table WHERE foo > 1" );
    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaInteger( "foo" ) );
    sql.parse( rowMeta );

    SqlTransGenerator generator = new SqlTransGenerator( sql, 0 );

    SelectValuesMeta selectValuesMeta = getSelectStepValuesMeta( generator.generateTransMeta() );
    assertThat( selectValuesMeta.getSelectName(), equalTo( new String[] { "foo" } ) );
    assertThat( selectValuesMeta.getSelectRename(), equalTo( new String[] { null } ) );
  }

  @Test
  public void testUniqueStep() throws KettleException {
    SQL sql = new SQL( "SELECT DISTINCT foo FROM table" );
    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaInteger( "foo" ) );
    sql.parse( rowMeta );

    SqlTransGenerator generator = new SqlTransGenerator( sql, 0 );

    SelectValuesMeta selectValuesMeta = getSelectStepValuesMeta( generator.generateTransMeta() );
    assertThat( selectValuesMeta.getSelectName(), equalTo( new String[] { "foo" } ) );
    assertThat( selectValuesMeta.getSelectRename(), equalTo( new String[] { null } ) );
  }

  @Test
  public void testDataFormatting() throws Exception {
    SQL sql = new SQL( "SELECT * FROM table" );
    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "str" ) );
    rowMeta.addValueMeta( new ValueMetaDate( "time" ) );
    rowMeta.addValueMeta( new ValueMetaInteger( "long" ) );
    sql.parse( new ValueMetaResolver( rowMeta ).getRowMeta() );

    SqlTransGenerator generator = new SqlTransGenerator( sql, -1 );

    TransMeta transMeta = generator.generateTransMeta();
    RowMetaInterface outputFields = transMeta.getStepFields( generator.getResultStepName() );

    Calendar calendar = Calendar.getInstance();
    calendar.clear();
    calendar.set( 2016, Calendar.FEBRUARY, 12, 13, 20 );

    Object[] row = { "value", calendar.getTime(), 42L };

    assertThat( outputFields.getFieldNames(), arrayContaining( "str", "time", "long" ) );
    assertThat( outputFields.getString( row, 0 ), is( "value" ) );
    assertThat( outputFields.getString( row, 1 ), is( "2016-02-12" ) );
    assertThat( outputFields.getString( row, 2 ), is( "42" ) );
  }

  @Test
  public void testRowLimit() throws KettleException {
    SQL sql = new SQL( "SELECT * FROM table" );
    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "foo" ) );
    sql.parse( rowMeta );

    SqlTransGenerator generator = new SqlTransGenerator( sql, 1 );

    SelectValuesMeta selectValuesMeta = getSelectStepValuesMeta( generator.generateTransMeta() );
    assertThat( selectValuesMeta.getSelectName(), equalTo( new String[] { "foo" } ) );
    assertThat( selectValuesMeta.getSelectRename(), equalTo( new String[] { null } ) );
  }

  @Test
  public void testServiceLimit() throws KettleException {
    SQL sql = new SQL( "SELECT * FROM table" );
    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "foo" ) );
    sql.parse( rowMeta );

    final String genLimitStep = "Limit input rows";

    SqlTransGenerator generator = new SqlTransGenerator( sql, 0, 2 );

    TransMeta transMeta = generator.generateTransMeta();
    SampleRowsMeta limitInput = (SampleRowsMeta) getStepByName( transMeta, genLimitStep );
    assertEquals( "limit not generated", "1..2", limitInput.getLinesRange() );

    // bad value
    generator = new SqlTransGenerator( sql, 0, -3 );
    transMeta = generator.generateTransMeta();
    assertTrue( "limit<=0 not ignored", Arrays.asList( transMeta.getStepNames() ).indexOf( genLimitStep ) < 0 );
  }
}

