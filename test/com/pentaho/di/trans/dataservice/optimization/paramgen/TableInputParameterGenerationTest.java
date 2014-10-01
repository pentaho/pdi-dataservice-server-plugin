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
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaAndData;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.steps.tableinput.TableInput;
import org.pentaho.di.trans.steps.tableinput.TableInputData;

import static com.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGenerationTest.AND;
import static com.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGenerationTest.OR;
import static com.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGenerationTest.newCondition;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TableInputParameterGenerationTest {
  @Mock private TableInput stepInterface;
  @Mock private Database database;
  @Mock private DatabaseMeta databaseMeta;
  private TableInputParameterGeneration service;

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
    TableInputData data = new TableInputData();
    data.db = database;
    when( stepInterface.getStepDataInterface() ).thenReturn( data );

    service = new TableInputParameterGeneration();
  }

  @Test
  public void testPushDown() throws Exception {
    // Add filters to both WHERE and GROUP by converting original query to a prepared statement...
    String originalQuery =
      "SELECT DepartmentName, COUNT(*) as EmployeeCount "
        + "FROM Department, Employee "
        + "WHERE Employee.DepartmentId = Department.DepartmentId ${EMPLOYEE_FILTER} ";

    ParameterGeneration employeeFilterParamGen = new ParameterGeneration();
    employeeFilterParamGen.setParameterName( "EMPLOYEE_FILTER" );
    employeeFilterParamGen.setForm( OptimizationForm.FILTER_CLAUSE );

    // Employee.Grade = "G7"
    Condition employeeFilter = newCondition( "Employee.Grade", "G7" );

    // Push Down condition
    service.pushDown( employeeFilter, employeeFilterParamGen, stepInterface );

    // The employee filter variable should now be set
    ArgumentCaptor<String> varCaptor = ArgumentCaptor.forClass( String.class );
    verify( stepInterface ).setVariable( eq( employeeFilterParamGen.getParameterName() ), varCaptor.capture() );

    // Update original query with variable values
    Variables variables = new Variables();
    variables.setVariable( employeeFilterParamGen.getParameterName(), varCaptor.getValue() );
    String resultQuery = variables.environmentSubstitute( originalQuery );

    String expectedQuery =
      "SELECT DepartmentName, COUNT(*) as EmployeeCount "
        + "FROM Department, Employee "
        + "WHERE Employee.DepartmentId = Department.DepartmentId AND \"Employee.Grade\" = 'G7' ";
    assertThat( resultQuery, equalTo( expectedQuery ) );
  }

  @Test
  public void testConvertAtomicCondition() throws KettleValueException, PushDownOptimizationException {
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

  private void testFunctionType( int function, String expected, Object value ) throws KettleValueException, PushDownOptimizationException {
    ValueMetaAndData right_exact = new ValueMetaAndData( "mock_value", value );
    Condition condition = new Condition( "field_name", function, null, right_exact );
    assertThat( service.convertAtomicCondition( condition, database ), equalTo( expected ) );
  }

  @Test
  public void testConvertCondition() throws Exception {
    // ( ( A | B ) & C )
    Condition condition = new Condition();
    condition.addCondition( newCondition( "A", "valA" ) );
    condition.getCondition( 0 ).addCondition( newCondition( OR, "B", 32 ) );
    condition.addCondition( newCondition( AND, "C", "valC" ) );

    StringBuilder sqlBuilder = new StringBuilder();

    service.convertCondition( condition, sqlBuilder, database );

    assertThat( sqlBuilder.toString(), equalTo( "( ( \"A\" = 'valA' OR \"B\" = 32 ) AND \"C\" = 'valC' )" ) );
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
