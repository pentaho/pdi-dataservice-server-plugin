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
import com.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
import com.pentaho.di.trans.dataservice.optimization.mongod.MongodbPredicate;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pentaho.di.core.Condition;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

import static org.mockito.Mockito.*;

public class MongodbInputParameterGenerationTest extends MongodbInputParameterGeneration {

  @Mock Condition condition;

  @Mock ParameterGeneration parameterGeneration;

  @Mock StepInterface stepInterface;

  @Mock MongodbPredicate mongodbPredicate;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks( this );
    when( stepInterface.getStepMeta() ).thenReturn( mock( StepMeta.class ) );
  }

  @Test( expected = PushDownOptimizationException.class )
  public void testPushDownThrowsWithInvalidType() throws Exception {
    when( stepInterface.getStepMeta().getTypeId() ).thenReturn( "InvalidType" );
    pushDown( condition, parameterGeneration, stepInterface );
  }

  @Test
  public void testPushDownSetsParameter() throws Exception {
    when( stepInterface.getStepMeta().getTypeId() ).thenReturn( "MongoDbInput" );
    when( mongodbPredicate.asFilterCriteria() ).thenReturn(  "mockedFilter" );
    when( parameterGeneration.getParameterName() ).thenReturn( "paramName" );
    pushDown( condition, parameterGeneration, stepInterface );
    verify( stepInterface ).setVariable( "paramName", "mockedFilter" );
  }

  @Override
  protected MongodbPredicate getMongodbPredicate( Condition condition ) {
    return mongodbPredicate;
  }
}
