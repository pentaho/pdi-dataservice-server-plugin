/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;

import java.util.Arrays;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class SqlTransGeneratorTest {

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
    int selectValuesIndex = Arrays.asList( transMeta.getStepNames() ).indexOf( "Select values" );
    if ( selectValuesIndex < 0 ) {
      fail( "Expected a step named 'Select values'" );
    }
    return (SelectValuesMeta) transMeta.getStep( selectValuesIndex ).getStepMetaInterface();
  }

}
