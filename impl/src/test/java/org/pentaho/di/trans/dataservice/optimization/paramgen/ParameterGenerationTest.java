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


package org.pentaho.di.trans.dataservice.optimization.paramgen;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.attributes.metastore.EmbeddedMetaStore;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.parameters.DuplicateParamException;
import org.pentaho.di.core.row.ValueMetaAndData;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.core.sql.SQLCondition;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.OptimizationImpactInfo;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationException;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.SourceTargetFields;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class ParameterGenerationTest {

  public static final String DATA_SERVICE_NAME = "My Data Service";
  public static final int AND = Condition.OPERATOR_AND;
  public static final int OR = Condition.OPERATOR_OR;
  public static final String PARAM_NAME = "MY_INJECTED_PARAM";
  public static final String OPT_NAME = "My Optimization";
  public static final String OPT_STEP = "Optimized Step";
  public static final String EXPECTED_DEFAULT = "## PARAMETER DEFAULT ##";

  private ParameterGeneration paramGen;
  @Mock private ParameterGenerationFactory serviceProvider;
  @Mock private ParameterGenerationService service;
  @Mock private TransMeta transMeta;
  @Mock private StepInterface stepInterface;
  @Mock private StepMeta stepMeta;
  @Mock private EmbeddedMetaStore embeddedMetaStore;
  @Mock private DataServiceExecutor executor;

  @Before
  public void setup() {
    paramGen = new ParameterGeneration( serviceProvider );
    paramGen.setParameterName( PARAM_NAME );
    paramGen.createFieldMapping( "A_src", "A_tgt" );
    paramGen.createFieldMapping( "B_src", "B_tgt" );
    paramGen.createFieldMapping( "C_src", "C_tgt" );

    when( transMeta.findStep( OPT_STEP ) ).thenReturn( stepMeta );
    when( stepInterface.getStepMeta() ).thenReturn( stepMeta );
    when( serviceProvider.getService( stepMeta ) ).thenReturn( service );
    when( service.getParameterDefault() ).thenReturn( EXPECTED_DEFAULT );
  }


  @Test
  public void testCRUDFieldMapping() throws Exception {
    ParameterGeneration parameterGeneration = new ParameterGeneration( serviceProvider );
    // Get live view of field mappings
    List<SourceTargetFields> fieldMappings = parameterGeneration.getFieldMappings();

    assertEquals( 0, fieldMappings.size() );

    SourceTargetFields mapping = parameterGeneration.createFieldMapping();

    assertEquals( 1, fieldMappings.size() );
    assertTrue( fieldMappings.contains( mapping ) );

    parameterGeneration.removeFieldMapping( mapping );
    assertEquals( 0, fieldMappings.size() );
  }

  @Test
  public void testInit() throws DuplicateParamException {
    DataServiceMeta dataService = mock( DataServiceMeta.class );
    PushDownOptimizationMeta pdo = new PushDownOptimizationMeta();
    pdo.setName( OPT_NAME );
    pdo.setStepName( OPT_STEP );

    when( transMeta.findStep( OPT_STEP ) ).thenReturn( null, stepMeta );
    when( serviceProvider.getService( stepMeta ) ).thenReturn( null, service );

    paramGen.init( transMeta, dataService, pdo ); // Step not found
    paramGen.init( transMeta, dataService, pdo ); // Service not found
    paramGen.init( transMeta, dataService, pdo ); // Okay
    verify( transMeta, times( 2 ) ).addParameterDefinition( eq( PARAM_NAME ), eq( "" ), anyString() );
    verify( transMeta ).addParameterDefinition( eq( PARAM_NAME ), eq( EXPECTED_DEFAULT ), anyString() );

    doThrow( DuplicateParamException.class ).when( transMeta ).addParameterDefinition( eq( PARAM_NAME ),
      eq( EXPECTED_DEFAULT ), anyString() );
    paramGen.init( transMeta, dataService, pdo ); //Exception should be silently caught
    verify( transMeta, atLeast( 3 ) ).activateParameters();
  }

  @Test
  public void testActivationFailure() throws Exception {
    SQL query = mockSql( newCondition( "A_src" ) );
    when( executor.getSql() ).thenReturn( query );
    // All okay
    assertTrue( paramGen.activate( executor, stepInterface ) );

    // Parameter value is blank
    paramGen.setParameterName( "" );
    assertFalse( paramGen.activate( executor, stepInterface ) );
    paramGen.setParameterName( PARAM_NAME );
    assertTrue( paramGen.activate( executor, stepInterface ) );

    // Service throws an error the first time
    doThrow( PushDownOptimizationException.class )
      .doNothing()
      .when( service ).pushDown( any( Condition.class ), same( paramGen ), same( stepInterface ) );
    assertFalse( paramGen.activate( executor, stepInterface ) );
    assertTrue( paramGen.activate( executor, stepInterface ) );

    // Step type is not supported
    when( serviceProvider.getService( stepMeta ) ).thenReturn( null, service );
    assertFalse( paramGen.activate( executor, stepInterface ) );
    assertTrue( paramGen.activate( executor, stepInterface ) );

    // Query does not have a WHERE clause
    when( executor.getSql() ).thenReturn( mock( SQL.class ) );
    assertFalse( paramGen.activate( executor, stepInterface ) );

    // Query could not be mapped
    query = mockSql( newCondition( "UNMAPPED" ) );
    when( executor.getSql() ).thenReturn( query );
    assertFalse( paramGen.activate( executor, stepInterface ) );
  }

  @Test
  public void testActivation() throws Exception {
    // ( A & ( B | C ) )
    Condition condition = newCondition( "A_src", "A_value" );
    condition.addCondition( newCondition( AND, "B_src", "B_value" ) );
    condition.getCondition( 1 ).addCondition( newCondition( OR, "C_src", "C_value" ) );
    SQL query = mockSql( condition );
    when( executor.getSql() ).thenReturn( query );

    assertTrue( paramGen.activate( executor, stepInterface ) );

    ArgumentCaptor<Condition> pushDownCaptor = ArgumentCaptor.forClass( Condition.class );
    verify( service ).pushDown( pushDownCaptor.capture(), same( paramGen ), same( stepInterface ) );

    Condition verify = pushDownCaptor.getValue();
    assertEquals( "A_tgt", verify.getCondition( 0 ).getLeftValuename() );
    assertEquals( "A_value", verify.getCondition( 0 ).getRightExactString() );
    assertEquals( AND, verify.getCondition( 1 ).getOperator() );
    assertEquals( "B_tgt", verify.getCondition( 1 ).getCondition( 0 ).getLeftValuename() );
    assertEquals( "B_value", verify.getCondition( 1 ).getCondition( 0 ).getRightExactString() );
    assertEquals( OR, verify.getCondition( 1 ).getCondition( 1 ).getOperator() );
    assertEquals( "C_tgt", verify.getCondition( 1 ).getCondition( 1 ).getLeftValuename() );
    assertEquals( "C_value", verify.getCondition( 1 ).getCondition( 1 ).getRightExactString() );
  }

  @Test
  public void testConditionMapping() throws Exception {
    Condition condition, verify;
    // In examples below, fields A, B, and C are mapped

    //  ( A & Z ) -> ( A )
    condition = newCondition( "A_src" );
    condition.addCondition( newCondition( AND, "Z" ) );
    assertNotNull( condition.toString(), verify = paramGen.mapConditionFields( condition ) );
    assertEquals( "A_tgt", verify.getLeftValuename() );
    assertEquals( 0, verify.getChildren().size() );

    // ( A & ( B & C ) ) -> ( A & ( B & C ) )
    condition = newCondition( "A_src" );
    condition.addCondition( newCondition( AND, "B_src" ) );
    condition.getCondition( 1 ).addCondition( newCondition( AND, "C_src" ) );
    assertNotNull( condition.toString(), verify = paramGen.mapConditionFields( condition ) );
    assertEquals( "A_tgt", verify.getCondition( 0 ).getLeftValuename() );
    assertEquals( AND, verify.getCondition( 1 ).getOperator() );
    assertEquals( "B_tgt", verify.getCondition( 1 ).getCondition( 0 ).getLeftValuename() );
    assertEquals( AND, verify.getCondition( 1 ).getCondition( 1 ).getOperator() );
    assertEquals( "C_tgt", verify.getCondition( 1 ).getCondition( 1 ).getLeftValuename() );

    // ( ( A & B ) | ( A & C ) ) -> ( ( A & B ) | ( A & C ) )
    condition = new Condition();
    condition.addCondition( newCondition( "A_src" ) );
    condition.getCondition( 0 ).addCondition( newCondition( AND, "B_src" ) );
    condition.addCondition( newCondition( OR, "A_src" ) );
    condition.getCondition( 1 ).addCondition( newCondition( AND, "C_src" ) );
    assertNotNull( condition.toString(), verify = paramGen.mapConditionFields( condition ) );
    assertEquals( "A_tgt", verify.getCondition( 0 ).getCondition( 0 ).getLeftValuename() );
    assertEquals( AND, verify.getCondition( 0 ).getCondition( 1 ).getOperator() );
    assertEquals( "B_tgt", verify.getCondition( 0 ).getCondition( 1 ).getLeftValuename() );
    assertEquals( OR, verify.getCondition( 1 ).getOperator() );
    assertEquals( "A_tgt", verify.getCondition( 1 ).getCondition( 0 ).getLeftValuename() );
    assertEquals( AND, verify.getCondition( 1 ).getCondition( 1 ).getOperator() );
    assertEquals( "C_tgt", verify.getCondition( 1 ).getCondition( 1 ).getLeftValuename() );

    // ( ( A | B ) & ( A | C ) ) -> ( ( A | B ) & ( A | C ) )
    condition = new Condition();
    condition.addCondition( newCondition( "A_src" ) );
    condition.getCondition( 0 ).addCondition( newCondition( OR, "B_src" ) );
    condition.addCondition( newCondition( AND, "A_src" ) );
    condition.getCondition( 1 ).addCondition( newCondition( OR, "C_src" ) );
    assertNotNull( condition.toString(), verify = paramGen.mapConditionFields( condition ) );
    assertEquals( "A_tgt", verify.getCondition( 0 ).getCondition( 0 ).getLeftValuename() );
    assertEquals( OR, verify.getCondition( 0 ).getCondition( 1 ).getOperator() );
    assertEquals( "B_tgt", verify.getCondition( 0 ).getCondition( 1 ).getLeftValuename() );
    assertEquals( AND, verify.getCondition( 1 ).getOperator() );
    assertEquals( "A_tgt", verify.getCondition( 1 ).getCondition( 0 ).getLeftValuename() );
    assertEquals( OR, verify.getCondition( 1 ).getCondition( 1 ).getOperator() );
    assertEquals( "C_tgt", verify.getCondition( 1 ).getCondition( 1 ).getLeftValuename() );

    // ( ( A | B ) & ( C | Z ) & ( X & Y ) ) -> ( A | B )
    condition = new Condition();
    condition.addCondition( newCondition( "A_src" ) );
    condition.getCondition( 0 ).addCondition( newCondition( OR, "B_src" ) );
    condition.addCondition( newCondition( AND, "C_src" ) );
    condition.getCondition( 1 ).addCondition( newCondition( OR, "Z" ) );
    condition.addCondition( newCondition( AND, "X" ) );
    condition.getCondition( 2 ).addCondition( newCondition( AND, "Y" ) );
    assertNotNull( condition.toString(), verify = paramGen.mapConditionFields( condition ) );
    assertEquals( "A_tgt", verify.getCondition( 0 ).getCondition( 0 ).getLeftValuename() );
    assertEquals( OR, verify.getCondition( 0 ).getCondition( 1 ).getOperator() );
    assertEquals( "B_tgt", verify.getCondition( 0 ).getCondition( 1 ).getLeftValuename() );
    assertEquals( 1, verify.getChildren().size() );

    // ( A & ( B | Z ) ) -> A
    condition = newCondition( "A_src" );
    condition.addCondition( newCondition( AND, "B_src" ) );
    condition.getCondition( 1 ).addCondition( newCondition( OR, "Z" ) );
    assertNotNull( condition.toString(), verify = paramGen.mapConditionFields( condition ) );
    assertEquals( "A_tgt", verify.getLeftValuename() );
    assertEquals( 0, verify.getChildren().size() );

    // ( Z || A ) -> none
    condition = newCondition( "Z" );
    condition.addCondition( newCondition( OR, "A_src" ) );
    assertEquals( condition.toString(), null, paramGen.mapConditionFields( condition ) );

    // ( A || Z ) -> none
    condition = newCondition( "A_src" );
    condition.addCondition( newCondition( OR, "Z" ) );
    assertNull( condition.toString(), paramGen.mapConditionFields( condition ) );

    // !( A & Z ) -> none
    condition = newCondition( "A_src" );
    condition.addCondition( newCondition( AND, "Z" ) );
    condition.negate();
    assertNull( condition.toString(), paramGen.mapConditionFields( condition ) );

    //  !( A || Z ) -> !A
    condition = newCondition( "A_src" );
    condition.addCondition( newCondition( OR, "Z" ) );
    condition.negate();
    assertNotNull( condition.toString(), verify = paramGen.mapConditionFields( condition ) );
    assertEquals( "A_tgt", verify.getLeftValuename() );
    assertEquals( 0, verify.getChildren().size() );
    assertTrue( verify.isNegated() );
  }

  @Test
  public void testOperatorHandlingWithPartiallyApplicableCondition() throws Exception {
    // Verifies no dangling AND/OR operators are left around after unmapped fields
    // have been removed.
    Condition condition, verify;
    // In examples below, fields A, B, and C are mapped

    //  ( Z & A ) -> ( A )
    condition = newCondition( "Z" );
    condition.addCondition( newCondition( AND, "A_src" ) );
    assertNotNull( condition.toString(), verify = paramGen.mapConditionFields( condition ) );
    assertEquals( "A_tgt", verify.getLeftValuename() );
    assertEquals( 0, verify.getChildren().size() );
    assertEquals( "Only one predicate, should be OPERATOR_NONE",
      Condition.OPERATOR_NONE, verify.getOperator() );

    //  ( Z & ( A | B ) ) -> ( A | B )
    condition = newCondition( "Z" );
    condition.addCondition( newCondition( AND, "A_src" ) );
    condition.getCondition( 1 ).addCondition( newCondition( OR, "B_src" ) );
    assertNotNull( condition.toString(), paramGen.mapConditionFields( condition ) );

    //  ( Z & A & B ) -> ( A & B )
    //  3 flat conditions
    condition = newCondition( "Z" );
    condition.addCondition( newCondition( AND, "A_src" ) );
    condition.addCondition( newCondition( AND, "B_src" ) );
    assertNotNull( condition.toString(), paramGen.mapConditionFields( condition ) );

    //  ( B & ( Z & A ) ) -> ( B & A )
    // unmapped condition as first child in nested expression
    condition = newCondition( "B_src" );
    condition.addCondition( newCondition( AND, "Z" ) );
    condition.getCondition( 1 ).addCondition( newCondition( AND, "A_src" ) );
    assertNotNull( condition.toString(), paramGen.mapConditionFields( condition ) );

    //  !( Z || A || B ) -> !( A || B )
    //  3 flat conditions, negated
    condition = newCondition( "Z" );
    condition.addCondition( newCondition( OR, "A_src" ) );
    condition.addCondition( newCondition( OR, "B_src" ) );
    condition.negate();
    assertNotNull( condition.toString(), verify = paramGen.mapConditionFields( condition ) );
    assertEquals( 2, verify.getChildren().size() );
    assertEquals( "A_tgt", verify.getCondition( 0 ).getLeftValuename() );
    assertEquals( OR, verify.getCondition( 1 ).getOperator() );
    assertEquals( "B_tgt", verify.getCondition( 1 ).getLeftValuename() );
    assertTrue( verify.isNegated() );
  }

  @Test
  public void testPreview() throws KettleValueException, PushDownOptimizationException {
    Condition condition = newCondition( "A_src", "A_value" );
    SQL query = mockSql( condition );
    when( executor.getSql() ).thenReturn( query );

    OptimizationImpactInfo optImpact = mock( OptimizationImpactInfo.class );
    when( service.preview( any( Condition.class ), same( paramGen ), same( stepInterface ) ) ).thenReturn( optImpact );
    assertEquals( optImpact, paramGen.preview( executor, stepInterface ) );

    ArgumentCaptor<Condition> pushDownCaptor = ArgumentCaptor.forClass( Condition.class );
    verify( service ).preview( pushDownCaptor.capture(), same( paramGen ), same( stepInterface ) );

    Condition verify = pushDownCaptor.getValue();
    assertEquals( "A_tgt", verify.getLeftValuename() );
    assertEquals( "A_value", verify.getRightExactString() );
  }

  public static Condition newCondition( String lhs ) throws KettleValueException {
    return newCondition( lhs, "value" );
  }

  public static Condition newCondition( String lhs, Object value ) throws KettleValueException {
    ValueMetaAndData right_exact = new ValueMetaAndData( value.toString(), value );
    return new Condition( lhs, Condition.FUNC_EQUAL, value.toString(), right_exact );
  }

  public static Condition newCondition( int op, String lhs ) throws KettleValueException {
    Condition condition = newCondition( lhs );
    condition.setOperator( op );
    return condition;
  }

  public static Condition newCondition( int op, String lhs, Object value ) throws KettleValueException {
    ValueMetaAndData right_exact = new ValueMetaAndData( value.toString(), value );
    return new Condition( op, lhs, Condition.FUNC_EQUAL, value.toString(), right_exact );
  }

  public SQL mockSql( Condition condition ) {
    SQL query = mock( SQL.class );
    when( query.getWhereCondition() ).thenReturn( mock( SQLCondition.class ) );
    when( query.getWhereCondition().getCondition() ).thenReturn( condition );
    return query;
  }
}
