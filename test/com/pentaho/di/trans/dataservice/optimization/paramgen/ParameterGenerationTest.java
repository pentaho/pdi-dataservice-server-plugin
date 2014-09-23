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
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import com.pentaho.di.trans.dataservice.optimization.SourceTargetFields;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaAndData;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.core.sql.SQLCondition;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.steps.tableinput.TableInput;
import org.pentaho.di.trans.steps.tableinput.TableInputData;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ParameterGenerationTest {

  public static final String DATA_SERVICE_NAME = "My Data Service";
  public static final int AND = Condition.OPERATOR_AND;
  public static final int OR = Condition.OPERATOR_OR;
  public static final String PARAM_NAME = "MY_INJECTED_PARAM";

  private ParameterGeneration paramGen;

  @Before
  public void setup() {
    paramGen = new ParameterGeneration();
    paramGen.setParameterName( PARAM_NAME );
    paramGen.setForm( OptimizationForm.WHERE_CLAUSE );
    paramGen.createFieldMapping( "A_src", "A_tgt" );
    paramGen.createFieldMapping( "B_src", "B_tgt" );
    paramGen.createFieldMapping( "C_src", "C_tgt" );
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

  private static final String OPT_NAME = "My Optimization";
  private static final String OPT_STEP = "Optimized Step";

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
    assertEquals( expectedType.getForm(), verifyType.getForm() );
    assertEquals( expectedType.getFormName(), OptimizationForm.WHERE_CLAUSE.getFormName() );
    assertFalse( verifyType.getFieldMappings().isEmpty() );
    SourceTargetFields verifyMapping = verifyType.getFieldMappings().get( 0 );

    assertEquals( expectedMapping.getSourceFieldName(), verifyMapping.getSourceFieldName() );
    assertEquals( expectedMapping.getTargetFieldName(), verifyMapping.getTargetFieldName() );
  }

  @Test
  public void testActivationFailure() throws Exception {
    DataServiceExecutor executor = mock( DataServiceExecutor.class );
    Trans trans = mock( Trans.class );
    StepInterface stepInterface = mock( StepInterface.class );
    SQL query = mock( SQL.class );

    // Step type is not supported
    assertFalse( paramGen.activate( executor, trans, stepInterface, query ) );

    stepInterface = mockTableInputStep( mock( Database.class ) );

    // Query does not have a WHERE clause
    assertFalse( paramGen.activate( executor, trans, stepInterface, query ) );

    query = mockSql( newCondition( "Z" ) );

    // Query could not be mapped
    assertFalse( paramGen.activate( executor, trans, stepInterface, query ) );

    query = mockSql( newCondition( "A_src" ) );

    // All okay
    assertTrue( paramGen.activate( executor, trans, stepInterface, query ) );
  }

  @Test
  public void testTableInputActivation() throws Exception {
    DataServiceExecutor executor = mock( DataServiceExecutor.class );

    Trans trans = mock( Trans.class );

    Database database = mock( Database.class );
    TableInput step = mockTableInputStep( database );

    Condition condition = newCondition( "A_src" );
    condition.addCondition( newCondition( AND, "B_src" ) );
    condition.getCondition( 1 ).addCondition( newCondition( OR, "C_src" ) );
    SQL query = mockSql( condition );

    assertTrue( paramGen.activate( executor, trans, step, query ) );

    ArgumentCaptor<String> paramValue = ArgumentCaptor.forClass( String.class );
    verify( step ).setVariable( eq( PARAM_NAME ), paramValue.capture() );

    String expectedValue = "WHERE ( \"A_tgt\" = 'value' AND ( \"B_tgt\" = 'value' OR \"C_tgt\" = 'value' ) )";
    assertArrayEquals( StringUtils.split( expectedValue ), StringUtils.split( paramValue.getValue() ) );
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
    assertEquals( "\"A_tgt\" = 'value'", paramGen.convertCondition( verify, null ) );

    //  ( Z & ( A | B ) ) -> ( A | B )
    condition = newCondition( "Z" );
    condition.addCondition( newCondition( AND, "A_src" ) );
    condition.getCondition( 1 ).addCondition( newCondition( OR, "B_src" ) );
    assertNotNull( condition.toString(), verify = paramGen.mapConditionFields( condition ) );
    assertEquals( " (  ( \"A_tgt\" = 'value' OR \"B_tgt\" = 'value' )  ) ",
      paramGen.convertCondition( verify, null ) );

    //  ( Z & A & B ) -> ( A & B )
    //  3 flat conditions
    condition = newCondition( "Z" );
    condition.addCondition( newCondition( AND, "A_src" ) );
    condition.addCondition( newCondition( AND, "B_src" ) );
    assertNotNull( condition.toString(), verify = paramGen.mapConditionFields( condition ) );
    assertEquals( " ( \"A_tgt\" = 'value' AND \"B_tgt\" = 'value' ) ",
      paramGen.convertCondition( verify, null ) );

    //  ( B & ( Z & A ) ) -> ( B & A )
    // unmapped condition as first child in nested expression
    condition = newCondition( "B_src" );
    condition.addCondition( newCondition( AND, "Z" ) );
    condition.getCondition( 1 ).addCondition( newCondition( AND, "A_src" ) );
    assertNotNull( condition.toString(), verify = paramGen.mapConditionFields( condition ) );
    assertEquals( " ( \"B_tgt\" = 'value' AND \"A_tgt\" = 'value' ) ",
      paramGen.convertCondition( verify, null ) );
  }

  private Condition newCondition( String lhs ) throws KettleValueException {
    ValueMetaAndData right_exact = new ValueMetaAndData( "mock_value", "value" );
    return new Condition( lhs, Condition.FUNC_EQUAL, "value", right_exact );
  }

  private Condition newCondition( int op, String lhs ) throws KettleValueException {
    Condition condition = newCondition( lhs );
    condition.setOperator( op );
    return condition;
  }

  public TableInput mockTableInputStep( Database database ) {
    TableInput step = mock( TableInput.class );
    TableInputData tableInputData = new TableInputData();
    when( step.getStepDataInterface() ).thenReturn( tableInputData );
    tableInputData.db = database;
    return step;
  }

  public SQL mockSql( Condition condition ) {
    SQL query = mock( SQL.class ); // ( A & ( B | C ) )
    when( query.getWhereCondition() ).thenReturn( mock( SQLCondition.class ) );
    when( query.getWhereCondition().getCondition() ).thenReturn( condition );
    return query;
  }
}
