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

import com.pentaho.di.trans.dataservice.DataServiceExecutor;
import com.pentaho.di.trans.dataservice.DataServiceMeta;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationException;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import com.pentaho.di.trans.dataservice.optimization.SourceTargetFields;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.parameters.DuplicateParamException;
import org.pentaho.di.core.row.ValueMetaAndData;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.core.sql.SQLCondition;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ParameterGenerationTest {

  public static final String DATA_SERVICE_NAME = "My Data Service";
  public static final int AND = Condition.OPERATOR_AND;
  public static final int OR = Condition.OPERATOR_OR;
  public static final String PARAM_NAME = "MY_INJECTED_PARAM";
  public static final String OPT_NAME = "My Optimization";
  public static final String OPT_STEP = "Optimized Step";
  public static final String EXPECTED_DEFAULT = "## PARAMETER DEFAULT ##";

  private ParameterGeneration paramGen;
  @Mock private ParameterGenerationServiceProvider serviceProvider;
  @Mock private ParameterGenerationService service;
  @Mock private Trans trans;
  @Mock private TransMeta transMeta;
  @Mock private StepInterface stepInterface;
  @Mock private StepMeta stepMeta;
  @Mock private DataServiceExecutor executor;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks( this );

    paramGen = new ParameterGeneration();
    paramGen.setParameterName( PARAM_NAME );
    paramGen.createFieldMapping( "A_src", "A_tgt" );
    paramGen.createFieldMapping( "B_src", "B_tgt" );
    paramGen.createFieldMapping( "C_src", "C_tgt" );
    paramGen.serviceProvider = serviceProvider;

    when( trans.getTransMeta() ).thenReturn( transMeta );
    when( transMeta.findStep( OPT_STEP ) ).thenReturn( stepMeta );
    when( stepInterface.getStepMeta() ).thenReturn( stepMeta );
    when( serviceProvider.getService( stepMeta ) ).thenReturn( service );
    when( service.getParameterDefault() ).thenReturn( EXPECTED_DEFAULT );
    when( executor.getServiceStep() ).thenReturn( stepInterface );
  }


  @Test
  public void testCRUDFieldMapping() throws Exception {
    ParameterGeneration parameterGeneration = new ParameterGeneration();
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
  public void testSaveLoad() throws Exception {
    MetaStoreFactory<DataServiceMeta> metaStoreFactory = DataServiceMeta
        .getMetaStoreFactory( new MemoryMetaStore(), "Test Namespace" );

    // Create parent data service
    DataServiceMeta expectedDataService = new DataServiceMeta();
    expectedDataService.setName( DATA_SERVICE_NAME );
    expectedDataService.setTransObjectId( UUID.randomUUID().toString() );
    expectedDataService.setStepname( "Service Output Step" );

    // Define optimization
    PushDownOptimizationMeta expectedOptimization = new PushDownOptimizationMeta( );
    expectedOptimization.setName( OPT_NAME );
    expectedOptimization.setStepName( OPT_STEP );
    expectedDataService.getPushDownOptimizationMeta().add( expectedOptimization );

    // Define optimization type
    ParameterGeneration expectedType = new ParameterGeneration();
    expectedType.setParameterName( "MY_PARAMETER" );
    expectedOptimization.setType( expectedType );

    // Define field mapping
    SourceTargetFields expectedMapping = new SourceTargetFields( "SOURCE", "TARGET" );
    expectedType.getFieldMappings().add( expectedMapping );

    // Attempt Save
    metaStoreFactory.saveElement( expectedDataService );

    // Verify 'something' was stored
    List<DataServiceMeta> loadedElements = metaStoreFactory.getElements();
    assertEquals( "No elements loaded", 1, loadedElements.size() );
    // Reload Data Service
    DataServiceMeta verifyDataService = metaStoreFactory.loadElement( DATA_SERVICE_NAME );

    // Assert Equality of data service optimization and optimization parameters
    assertEquals( expectedDataService.getName(), verifyDataService.getName() );
    assertEquals( expectedDataService.getStepname(), verifyDataService.getStepname() );
    assertEquals( expectedDataService.getTransObjectId(), verifyDataService.getTransObjectId() );

    assertFalse( verifyDataService.getPushDownOptimizationMeta().isEmpty() );
    PushDownOptimizationMeta verifyOptimization = verifyDataService.getPushDownOptimizationMeta().get( 0 );

    assertEquals( expectedOptimization.getName(), verifyOptimization.getName() );
    assertEquals( expectedOptimization.getStepName(), verifyOptimization.getStepName() );

    assertTrue( verifyOptimization.getType() instanceof ParameterGeneration );
    ParameterGeneration verifyType = (ParameterGeneration) verifyOptimization.getType();

    assertEquals( expectedType.getParameterName(), verifyType.getParameterName() );
    assertFalse( verifyType.getFieldMappings().isEmpty() );
    SourceTargetFields verifyMapping = verifyType.getFieldMappings().get( 0 );

    assertEquals( expectedMapping.getSourceFieldName(), verifyMapping.getSourceFieldName() );
    assertEquals( expectedMapping.getTargetFieldName(), verifyMapping.getTargetFieldName() );
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

    doThrow( DuplicateParamException.class ).when( transMeta ).addParameterDefinition( eq( PARAM_NAME ), eq( EXPECTED_DEFAULT ), anyString() );
    paramGen.init( transMeta, dataService, pdo ); //Exception should be silently caught
    verify( transMeta, atLeast( 3 ) ).activateParameters();
  }

  @Test
  public void testActivationFailure() throws Exception {
    SQL query = mockSql( newCondition( "A_src" ) );
    when( executor.getSql() ).thenReturn( query );
    // All okay
    assertTrue( paramGen.activate( executor ) );

    // Parameter value is blank
    paramGen.setParameterName( "" );
    assertFalse( paramGen.activate( executor ) );
    paramGen.setParameterName( PARAM_NAME );
    assertTrue( paramGen.activate( executor ) );

    // Service throws an error the first time
    doThrow( PushDownOptimizationException.class )
      .doNothing()
      .when( service ).pushDown( any( Condition.class ), same( paramGen ), same( stepInterface ) );
    assertFalse( paramGen.activate( executor ) );
    assertTrue( paramGen.activate( executor ) );

    // Step type is not supported
    when( serviceProvider.getService( stepMeta ) ).thenReturn( null, service );
    assertFalse( paramGen.activate( executor ) );
    assertTrue( paramGen.activate( executor ) );

    // Query does not have a WHERE clause
    when( executor.getSql() ).thenReturn( mock( SQL.class ) );
    assertFalse( paramGen.activate( executor ) );

    // Query could not be mapped
    query = mockSql( newCondition( "UNMAPPED" ) );
    when( executor.getSql() ).thenReturn( query );
    assertFalse( paramGen.activate( executor ) );
  }

  @Test
  public void testActivation() throws Exception {
    // ( A & ( B | C ) )
    Condition condition = newCondition( "A_src", "A_value" );
    condition.addCondition( newCondition( AND, "B_src", "B_value" ) );
    condition.getCondition( 1 ).addCondition( newCondition( OR, "C_src", "C_value" ) );
    SQL query = mockSql( condition );
    when( executor.getSql() ).thenReturn( query );

    assertTrue( paramGen.activate( executor ) );

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
