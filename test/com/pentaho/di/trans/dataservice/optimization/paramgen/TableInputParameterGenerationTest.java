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

package com.pentaho.di.trans.dataservice.optimization.paramgen;

import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.database.map.DatabaseConnectionMap;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaAndData;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.steps.tableinput.TableInput;
import org.pentaho.di.trans.steps.tableinput.TableInputData;

import java.sql.Connection;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static com.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGenerationTest.AND;
import static com.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGenerationTest.OR;
import static com.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGenerationTest.newCondition;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.matchers.JUnitMatchers.containsString;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TableInputParameterGenerationTest {

  public static final String MOCK_PARTITION_ID = "Mock Partition ID";
  public static final String MOCK_CONNECTION_GROUP = "Mock Connection Group";
  @Mock private TableInput stepInterface;
  @Mock private Database database;
  @Mock private DatabaseMeta databaseMeta;

  private TableInputParameterGeneration service;
  private TableInputData data;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks( this );
    when( database.getDatabaseMeta() ).thenReturn( databaseMeta );
    when( databaseMeta.quoteSQLString( anyString() ) ).thenAnswer( new Answer<Object>() {
      @Override
      public Object answer( InvocationOnMock invocation ) throws Throwable {
        return String.format( "'%s'", invocation.getArguments() [ 0 ] );
      }
    } );

    // Setup Mock Step and Data
    data = new TableInputData();
    data.db = new Database( stepInterface, mock( DatabaseMeta.class ) );
    // Add mock connection to connection map, prevent an actual connection attempt
    data.db.setConnection( mock( Connection.class ) );
    data.db.setConnectionGroup( MOCK_CONNECTION_GROUP );
    data.db.setPartitionId( MOCK_PARTITION_ID );
    when( stepInterface.getStepDataInterface() ).thenReturn( data );

    DatabaseConnectionMap connectionMap = DatabaseConnectionMap.getInstance();
    connectionMap.getMap().clear();
    connectionMap.storeDatabase( MOCK_CONNECTION_GROUP, MOCK_PARTITION_ID, data.db );

    service = new TableInputParameterGeneration();
  }

  @Test
  public void testPushDown() throws Exception {
    // Add filters to both WHERE and GROUP by converting original query to a prepared statement...
    String originalQuery =
      "SELECT DepartmentName, COUNT(*) as EmployeeCount "
        + "FROM Department, Employee "
        + "WHERE Employee.DepartmentId = Department.DepartmentId AND ${EMPLOYEE_FILTER} ";

    ParameterGeneration employeeFilterParamGen = new ParameterGeneration();
    employeeFilterParamGen.setParameterName( "EMPLOYEE_FILTER" );

    // Employee.Grade = "G7"
    Condition employeeFilter = newCondition( "Employee.Grade", "G7" );

    // Push Down condition
    service.pushDown( employeeFilter, employeeFilterParamGen, stepInterface );

    // Verify that the database for this step is now 'wrapped'
    assertThat( data.db, is( instanceOf( DatabaseWrapper.class ) ) );
    final DatabaseWrapper databaseWrapper = (DatabaseWrapper) data.db;

    // The employee filter variable should now be set
    ArgumentCaptor<String> varCaptor = ArgumentCaptor.forClass( String.class );
    verify( stepInterface ).setVariable( eq( employeeFilterParamGen.getParameterName() ), varCaptor.capture() );

    // Verify stored data for runtime injection
    final List<String> fragmentIds = varCaptor.getAllValues();
    assertThat( fragmentIds.size(), is( 1 ) );
    assertTrue( databaseWrapper.pushDownMap.keySet().containsAll( fragmentIds ) );

    // Update original query with variable values
    Variables variables = new Variables();
    variables.setVariable( employeeFilterParamGen.getParameterName(), fragmentIds.get( 0 ) );
    String runtimeQuery = variables.environmentSubstitute( originalQuery );

    for ( String fragment : fragmentIds ) {
      assertThat( runtimeQuery, containsString( fragment ) );
    }

    // During trans runtime, values and sql fragments will be injected
    RowMeta rowMeta = new RowMeta();
    List<Object> values = new LinkedList<Object>();

    String resultQuery = databaseWrapper.injectRuntime( databaseWrapper.pushDownMap, runtimeQuery, rowMeta, values );

    String expectedQuery =
      "SELECT DepartmentName, COUNT(*) as EmployeeCount "
        + "FROM Department, Employee "
        + "WHERE Employee.DepartmentId = Department.DepartmentId AND \"Employee.Grade\" = ? ";
    assertThat( resultQuery, equalTo( expectedQuery ) );

    ValueMetaInterface employeeValueMeta = employeeFilter.getRightExact().getValueMeta();
    assertThat( rowMeta.getValueMetaList(), equalTo( Arrays.asList( employeeValueMeta ) ) );
    assertThat( values, equalTo( Arrays.<Object>asList( "G7" ) ) );
  }

  @Test
  public void testConvertAtomicCondition() throws Exception {
    testFunctionType( Condition.FUNC_EQUAL, "\"field_name\" = 'value'", "value" );
    testFunctionType( Condition.FUNC_NOT_EQUAL, "\"field_name\" <> 'value'", "value" );
    testFunctionType( Condition.FUNC_NOT_EQUAL, "\"field_name\" <> 123", 123 );
    testFunctionType( Condition.FUNC_SMALLER, "\"field_name\" < 123", 123 );
    testFunctionType( Condition.FUNC_SMALLER_EQUAL, "\"field_name\" <= 123", 123 );
    testFunctionType( Condition.FUNC_LARGER, "\"field_name\" > 123", 123 );
    testFunctionType( Condition.FUNC_LARGER_EQUAL, "\"field_name\" >= 123", 123 );
    testFunctionType( Condition.FUNC_IN_LIST,
      "\"field_name\"  IN  ('value1','value2','value3')", "value1;value2;value3" );
    testFunctionType( Condition.FUNC_IN_LIST,
      "\"field_name\"  IN  ('val;ue1','value2','value3','val ue4')", "val\\;ue1;value2;value3;val ue4" );
    testFunctionType( Condition.FUNC_LARGER_EQUAL, "\"field_name\" >= 123", 123 );

    try {
      testFunctionType( Condition.FUNC_REGEXP, "\"field_name\" >= 123", "123" );
      fail( "Should have thrown exception" );
    } catch ( PushDownOptimizationException e ) {
      assertTrue(  e.getMessage().contains( "REGEXP" ) );
    }
    testFunctionType( Condition.FUNC_NULL, "\"field_name\" IS NULL ", null );
    testFunctionType( Condition.FUNC_NOT_NULL, "\"field_name\" IS NOT NULL ", null );
    testFunctionType( Condition.FUNC_LIKE, "\"field_name\" LIKE 'MAT%CH'", "MAT%CH" );

    try {
      testFunctionType( Condition.FUNC_EQUAL, "\"field_name\" = 'value'", null );
      fail( "Should have thrown exception" );
    } catch ( PushDownOptimizationException e ) {
      assertThat( e.getMessage(), notNullValue() );
    }
  }

  private void testFunctionType( int function, String expected, Object value ) throws Exception {
    ValueMetaAndData right_exact = new ValueMetaAndData( "mock_value", value );
    Condition condition = new Condition( "field_name", function, null, right_exact );
    RowMeta paramsMeta = mock( RowMeta.class );
    List<Object> params = mock( List.class );
    assertThat( service.convertAtomicCondition( condition, paramsMeta, params ), equalTo( expected ) );
    verify( paramsMeta ).addValueMeta( right_exact.getValueMeta() );
    verify( params ).add( value );
  }

  @Test
  public void testConvertCondition() throws Exception {
    // ( ( A | B ) & C )
    Condition condition = new Condition(), a, b, c;
    condition.addCondition( a = newCondition( "A", "valA" ) );
    condition.getCondition( 0 ).addCondition( b = newCondition( OR, "B", 32 ) );
    condition.addCondition( c = newCondition( AND, "C", "valC" ) );

    StringBuilder sqlBuilder = new StringBuilder();
    RowMeta rowMeta = new RowMeta();
    List<Object> values = new LinkedList<Object>();

    service.convertCondition( condition, sqlBuilder, rowMeta, values );

    assertThat( sqlBuilder.toString(), equalTo( "( ( \"A\" = ? OR \"B\" = ? ) AND \"C\" = ? )" ) );

    // Verify that ValueMeta and data were stored in order
    List<ValueMetaInterface> expectedMeta = new LinkedList<ValueMetaInterface>();
    for ( Condition expected : Arrays.asList( a, b, c ) ) {
      expectedMeta.add( expected.getRightExact().getValueMeta() );
    }
    assertThat( rowMeta.getValueMetaList(), equalTo( expectedMeta ) );
    assertThat( values, equalTo( Arrays.<Object>asList( "valA", 32, "valC" ) ) );
  }

  @Test public void testFailures() throws Exception {
    // Throw exception if type is not TableInput
    try {
      service.pushDown( mock( Condition.class ), mock( ParameterGeneration.class ), mock( StepInterface.class ) );
      fail();
    } catch ( PushDownOptimizationException thrown ) {
      assertThat( thrown.getMessage(), notNullValue() );
    }

    // Throw exception if connection fails
    KettleDatabaseException expected = new KettleDatabaseException();
    doThrow( expected ).when( database ).connect();

    try {
      service.pushDown( mock( Condition.class ), mock( ParameterGeneration.class ), stepInterface );
      fail();
    } catch ( PushDownOptimizationException thrown ) {
      assertThat( thrown.getCause(), equalTo( (Throwable) expected ) );
    }
  }
}
