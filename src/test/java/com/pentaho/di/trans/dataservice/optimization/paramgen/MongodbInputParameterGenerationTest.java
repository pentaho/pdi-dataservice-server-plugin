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
import com.pentaho.di.trans.dataservice.optimization.mongod.MongodbPredicate;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

import java.util.Map;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;
import static org.hamcrest.core.StringContains.containsString;
import static org.mockito.Mockito.*;

public class MongodbInputParameterGenerationTest extends MongodbInputParameterGeneration {

  private static final String TEST_JSON_QUERY = "{$limit : 3}, {$match : ${paramName}}";
  private static final java.lang.String TEST_STEP_XML =
    "  <step>\n"
      + "    <name>Test Step</name>"
      + "    <json_query>" + TEST_JSON_QUERY + "</json_query>"
      + "</step>\n";
  private static final java.lang.String MOCK_STEPNAME = "MOCK_STEPNAME";

  @Mock Condition condition;

  @Mock ParameterGeneration parameterGeneration;

  @Mock StepInterface stepInterface;

  @Mock MongodbPredicate mongodbPredicate;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks( this );
    when( stepInterface.getStepname() ).thenReturn( MOCK_STEPNAME );
    when( stepInterface.getStepMeta() ).thenReturn( mock( StepMeta.class ) );
    when( stepInterface.getStepMeta().getTypeId() ).thenReturn( "MongoDbInput" );
    when( stepInterface.getStepMeta().getXML() ).thenReturn( TEST_STEP_XML );
    when( mongodbPredicate.asFilterCriteria() ).thenReturn(  "mockedFilter" );
    when( parameterGeneration.getParameterName() ).thenReturn( "paramName" );
    when( parameterGeneration.setQueryParameter(
      any( String.class ), any( String.class ) ) ).thenReturn( "after optimization" );
    log = mock( LogChannelInterface.class );
  }

  @Test( expected = PushDownOptimizationException.class )
  public void testPushDownThrowsWithInvalidType() throws Exception {
    when( stepInterface.getStepMeta().getTypeId() ).thenReturn( "InvalidType" );
    pushDown( condition, parameterGeneration, stepInterface );
  }

  @Test
  public void testPushDownSetsParameter() throws Exception {
    pushDown( condition, parameterGeneration, stepInterface );
    verify( stepInterface ).setVariable( "paramName", "mockedFilter" );
  }

  @Test
  public void testPreviewWithModification() {
    OptimizationImpactInfo impact = preview( condition, parameterGeneration, stepInterface );
    assertThat( impact.getStepName(), equalTo( MOCK_STEPNAME ) );
    assertThat( impact.getQueryBeforeOptimization(),
      equalTo( TEST_JSON_QUERY ) );
    assertThat( impact.getQueryAfterOptimization(),
      equalTo( "after optimization" ) );
    assertTrue( impact.isModified() );
    assertThat( impact.getErrorMsg(), equalTo( "" ) );
  }

  @Test
  public void testPreviewWithoutModification() {
    when( parameterGeneration.getParameterName() ).thenReturn( "DifferentParamName" );
    when( parameterGeneration.setQueryParameter( TEST_JSON_QUERY, "mockedFilter" ) )
      .thenReturn( TEST_JSON_QUERY );

    OptimizationImpactInfo impact = preview( condition, parameterGeneration, stepInterface );
    assertThat( impact.getStepName(), equalTo( MOCK_STEPNAME ) );
    assertThat( impact.getQueryBeforeOptimization(),
      equalTo( TEST_JSON_QUERY ) );


    assertThat( impact.getQueryAfterOptimization(),
      equalTo( "" ) );
    assertFalse( impact.isModified() );
    assertThat( impact.getErrorMsg(), equalTo( "" ) );
  }

  @Test
  public void testPreviewWithNoQuery() throws KettleException {
    when( stepInterface.getStepMeta().getXML() ).thenReturn( "<empty/>" );
    when( parameterGeneration.setQueryParameter( "", "mockedFilter" ) )
      .thenReturn( "" );
    when( parameterGeneration.getParameterName() ).thenReturn( "paramName" );
    OptimizationImpactInfo impact = preview( condition, parameterGeneration, stepInterface );
    assertThat( impact.getStepName(), equalTo( MOCK_STEPNAME ) );
    assertThat( impact.getQueryBeforeOptimization(),
      equalTo( "<no query>" ) );
    assertThat( impact.getQueryAfterOptimization(),
      equalTo( "" ) );
    assertFalse( impact.isModified() );
    assertThat( impact.getErrorMsg(), equalTo( "" ) );
  }

  @Test
  public void testPreviewWithConversionFailure() throws KettleException {
    when( mongodbPredicate.asFilterCriteria() ).thenThrow( new PushDownOptimizationException( "FAILURE" ) );
    OptimizationImpactInfo impact = preview( condition, parameterGeneration, stepInterface );
    assertThat( impact.getStepName(), equalTo( MOCK_STEPNAME ) );
    assertThat( impact.getQueryBeforeOptimization(),
      equalTo( TEST_JSON_QUERY ) );
    assertFalse( impact.isModified() );
    assertThat( impact.getErrorMsg(), containsString( "FAILURE" )  );
  }


  @Override
  protected MongodbPredicate getMongodbPredicate( Condition condition, Map<String, String> fieldMappings ) {
    return mongodbPredicate;
  }
}
