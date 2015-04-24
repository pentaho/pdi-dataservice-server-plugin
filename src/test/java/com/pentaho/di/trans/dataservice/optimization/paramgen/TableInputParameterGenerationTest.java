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

package com.pentaho.di.trans.dataservice.optimization.paramgen;

import com.pentaho.di.trans.dataservice.optimization.OptimizationImpactInfo;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationException;
import com.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.database.map.DatabaseConnectionMap;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaAndData;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.tableinput.TableInput;
import org.pentaho.di.trans.steps.tableinput.TableInputData;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class TableInputParameterGenerationTest {

  public static final String MOCK_PARTITION_ID = "Mock Partition ID";
  public static final String MOCK_CONNECTION_GROUP = "Mock Connection Group";
  private ParameterGenerationFactory factory =
    new ParameterGenerationFactory( Collections.<ParameterGenerationServiceFactory>emptyList() );
  @Mock private TableInput stepInterface;
  @Mock private DatabaseMeta databaseMeta;
  @Mock private ValueMetaResolver resolver;

  private ValueMetaInterface resolvedValueMeta = new ValueMeta( "testValueMeta" );

  @InjectMocks
  private TableInputParameterGeneration service;
  private TableInputData data;

  @Before
  public void setUp() throws Exception {
    // Setup Mock Step and Data
    data = new TableInputData();
    when( stepInterface.getLogLevel() ).thenReturn( LogLevel.NOTHING );
    data.db = new Database( stepInterface, databaseMeta );
    // Add mock connection to connection map, prevent an actual connection attempt
    data.db.setConnection( mock( Connection.class ) );
    data.db.setConnectionGroup( MOCK_CONNECTION_GROUP );
    data.db.setPartitionId( MOCK_PARTITION_ID );
    when( stepInterface.getStepDataInterface() ).thenReturn( data );

    service.dbMeta = databaseMeta;

    when( databaseMeta.quoteField( anyString() ) ).thenAnswer(new Answer<String>() {
      @Override public String answer( InvocationOnMock invocation ) throws Throwable {
        return (String) invocation.getArguments()[ 0 ];
      }
    } );

    DatabaseConnectionMap connectionMap = DatabaseConnectionMap.getInstance();
    connectionMap.getMap().clear();
    connectionMap.storeDatabase( MOCK_CONNECTION_GROUP, MOCK_PARTITION_ID, data.db );

    setupValueMetaResolverMock();
  }

  private void setupValueMetaResolverMock() throws PushDownOptimizationException {
    when( resolver.getValueMeta( any( String.class ) ) )
      .thenReturn( resolvedValueMeta );
    when( resolver.getTypedValue( any( String.class ), any( Integer.class ), any() ) )
      .thenAnswer( new Answer<Object>() {
        @Override
        public Object answer( InvocationOnMock invocation ) throws Throwable {
          return invocation.getArguments()[2];
        }
      } );
    when( resolver.inListToTypedObjectArray( any( String.class ), any( String.class ) ) )
      .thenAnswer( new Answer<Object>() {
        @Override
        public Object answer( InvocationOnMock invocation ) throws Throwable {
          return Const.splitString( ( (String) invocation.getArguments()[ 1 ] ), ';', true );
        }
      } );
  }

  @Test
  public void testPushDown() throws Exception {
    // Add filters to both WHERE and GROUP by converting original query to a prepared statement...
    String originalQuery =
        "SELECT DepartmentName, COUNT(*) as EmployeeCount "
        + "FROM Department, Employee "
        + "WHERE Employee.DepartmentId = Department.DepartmentId AND ${EMPLOYEE_FILTER} ";

    ParameterGeneration employeeFilterParamGen = factory.createPushDown();
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
        + "WHERE Employee.DepartmentId = Department.DepartmentId AND Employee.Grade = ? ";
    assertThat( resultQuery, equalTo( expectedQuery ) );

    assertThat( rowMeta.getValueMetaList(), equalTo( Arrays.asList( resolvedValueMeta ) ) );
    assertThat( values, equalTo( Arrays.<Object>asList( "G7" ) ) );
  }


  @Test
  public void testPreview() throws KettleValueException, PushDownOptimizationException {
    ParameterGeneration param = factory.createPushDown();
    param.setParameterName( "param" );

    Condition employeeFilter = newCondition( "fooField", "barValue" );

    when( stepInterface.getStepMeta() ).thenReturn( mock( StepMeta.class ) );
    TableInputMeta mockTableInput = mock( TableInputMeta.class );
    String origQuery = "SELECT * FROM TABLE WHERE ${param}";
    when( mockTableInput.getSQL() ).thenReturn( "SELECT * FROM TABLE WHERE ${param}" );
    when( stepInterface.getStepMeta().getStepMetaInterface() ).thenReturn( mockTableInput );
    when( stepInterface.getStepname() ).thenReturn( "testStepName" );

    OptimizationImpactInfo impact = service.preview( employeeFilter, param, stepInterface );
    assertThat( impact.getQueryBeforeOptimization(),
      equalTo( origQuery ) );
    assertThat( impact.getStepName(),
      equalTo( "testStepName" ) );
    assertThat( impact.getQueryAfterOptimization(),
      equalTo( "Parameterized SQL:  SELECT * FROM TABLE WHERE fooField = ?   {1: barValue}" ) );
    assertTrue( impact.isModified() );
  }

  @Test
  public void testConvertAtomicCondition() throws Exception {
    testFunctionType( Condition.FUNC_EQUAL, "field_name = ?", "value" );
    testFunctionType( Condition.FUNC_NOT_EQUAL, "field_name <> ?", "value" );
    testFunctionType( Condition.FUNC_NOT_EQUAL, "field_name <> ?", 123 );
    testFunctionType( Condition.FUNC_SMALLER, "field_name < ?", 123 );
    testFunctionType( Condition.FUNC_SMALLER_EQUAL, "field_name <= ?", 123 );
    testFunctionType( Condition.FUNC_LARGER, "field_name > ?", 123 );
    testFunctionType( Condition.FUNC_LARGER_EQUAL, "field_name >= ?", 123 );

    testInListCondition( "value1;value2;value3", new String[] { "value1", "value2", "value3" },
      "field_name  IN  (?,?,?)" );
    testInListCondition("value1;value2;value3;val ue4" , new String[]{ "value1", "value2", "value3", "val ue4" }, "field_name  IN  (?,?,?,?)" );
    testFunctionType( Condition.FUNC_LARGER_EQUAL, "field_name >= ?", 123 );

    try {
      testFunctionType( Condition.FUNC_REGEXP, "field_name ~= ?", "123" );
      fail( "Should have thrown exception" );
    } catch ( PushDownOptimizationException e ) {
      assertTrue(  e.getMessage().contains( "REGEXP" ) );
    }
    testFunctionType( Condition.FUNC_NULL, "field_name IS NULL ", null );
    testFunctionType( Condition.FUNC_NOT_NULL, "field_name IS NOT NULL ", null );
    testFunctionType( Condition.FUNC_LIKE, "field_name LIKE ?", "MAT%CH" );

    try {
      testFunctionType( Condition.FUNC_EQUAL, "field_name = 'value'", null );
      fail( "Should have thrown exception" );
    } catch ( PushDownOptimizationException e ) {
      assertThat( e.getMessage(), notNullValue() );
    }
  }

  @SuppressWarnings( "unchecked" )
  protected void testInListCondition( String valueData, String[] inListExpectedValues, String expectedSql )
    throws KettleValueException, PushDownOptimizationException {
    ValueMetaAndData rightExactInList = new ValueMetaAndData( "mock_value", valueData );
    Condition inListCondition = new Condition( "field_name", Condition.FUNC_IN_LIST, null, rightExactInList );
    RowMeta inListParamsMeta = mock( RowMeta.class );
    List<Object> inListParams = mock( List.class );
    assertThat( service.convertAtomicCondition( inListCondition, inListParamsMeta, inListParams ), equalTo( expectedSql ) );
    for ( String inListValue : inListExpectedValues ) {
      verify( inListParams ).add( inListValue );
    }
    verify( inListParamsMeta, times( inListExpectedValues.length ) ).addValueMeta( resolvedValueMeta );
    verifyNoMoreInteractions( inListParams, inListParamsMeta );
  }

  @SuppressWarnings( "unchecked" )
  private void testFunctionType( int function, String expected, Object value ) throws Exception {
    ValueMetaAndData right_exact = new ValueMetaAndData( "mock_value", value );
    Condition condition = new Condition( "field_name", function, null, right_exact );
    RowMeta paramsMeta = mock( RowMeta.class );
    List<Object> params = mock( List.class );
    assertThat( service.convertAtomicCondition( condition, paramsMeta, params ), equalTo( expected ) );
    if ( value != null ) {
      verify( paramsMeta ).addValueMeta( resolvedValueMeta );
      verify( params ).add( value );
    }
    verifyNoMoreInteractions( paramsMeta, params );
  }

  @Test
  public void testConvertCondition() throws Exception {
    // ( ( A | B ) & !C )
    Condition condition = new Condition();
    condition.addCondition( newCondition( "A", "valA" ) );
    condition.getCondition( 0 ).addCondition( newCondition( OR, "B", 32 ) );
    condition.addCondition( newCondition( AND, "C", "valC" ) );
    condition.getCondition( 1 ).negate();

    StringBuilder sqlBuilder = new StringBuilder();
    RowMeta paramsMeta = mock( RowMeta.class );
    List<Object> values = new LinkedList<Object>();

    service.convertCondition( condition, sqlBuilder, paramsMeta, values );

    assertThat( sqlBuilder.toString(), equalTo( "( ( A = ? OR B = ? ) AND NOT C = ? )" ) );

    // Verify that resolved ValueMeta added 3 times, and data stored in order
    verify( paramsMeta, times( 3 ) ).addValueMeta( resolvedValueMeta );
    verify( databaseMeta, times( 3 ) ).quoteField( anyString() );
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
    data.db = mock( DatabaseWrapper.class );
    doThrow( expected ).when( data.db ).connect();

    try {
      service.pushDown( mock( Condition.class ), mock( ParameterGeneration.class ), stepInterface );
      fail();
    } catch ( PushDownOptimizationException thrown ) {
      assertThat( thrown.getCause(), equalTo( (Throwable) expected ) );
    }
  }

}
