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

package com.pentaho.di.trans.dataservice.validation;

import org.junit.Test;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class MongoDbBasicValidationTest extends BaseStepValidationTest {

  private MongoDbInputMeta mongoDbInputMeta;

  private MongoDbBasicValidation mongoDbBasicValidation = new MongoDbBasicValidation();

  @Override
  void init() {
    mongoDbInputMeta = mock( MongoDbInputMeta.class );
    when( stepMeta.getStepMetaInterface() ).thenReturn( mongoDbInputMeta );
  }

  @Test
  public void testIsApplicable() {
    assertTrue( mongoDbBasicValidation.supportsStep( stepMeta, log ) );
  }

  @Test
  public void testCheckNoParamJsonQueryisNull() {
    mongoDbBasicValidation.checkStep( checkStepsExtension, dataServiceMeta, log );

    // make sure something was added to remarks
    verify( remarks, times( 1 ) ).add( any( CheckResultInterface.class ) );
  }

  @Test
  public void testCheckJsonOutputType() {
    when( mongoDbInputMeta.getOutputJson() )
        .thenReturn( true );

    mongoDbBasicValidation.checkStep( checkStepsExtension, dataServiceMeta, log );

    // 2 remarks added, one for json output type, one for no param set.
    verify( remarks, times( 2 ) ).add( any( CheckResultInterface.class ) );
  }

  @Test
  public void testCheckNoParam() {
    when( mongoDbInputMeta.getJsonQuery() ).thenReturn(
        "{$unwind : 'foo'}" );
    mongoDbBasicValidation.checkStep( checkStepsExtension, dataServiceMeta, log );

    verify( remarks, times( 1 ) ).add( any( CheckResultInterface.class ) );
  }

  @Test
  public void testCheckNoWarningsWithParam() {
    when( mongoDbInputMeta.getJsonQuery() ).thenReturn(
        "{$unwind : 'foo'}, {$match : ${baz}}" );
    space.setVariable( "baz", "foo='123'" );
    mongoDbBasicValidation.checkStep( checkStepsExtension, dataServiceMeta, log );

    verify( remarks, never() ).add( any( CheckResultInterface.class ) );
  }

  @Test
  public void testCheckNoParamForDefinedOpt() {
    when( mongoDbInputMeta.getJsonQuery() ).thenReturn(
        "{$unwind : 'foo'}" );
    setupMockedParamGen( "testParam", "testStep" );
    when( stepMeta.getName() )
        .thenReturn( "testStep" );
    mongoDbBasicValidation.checkStep( checkStepsExtension, dataServiceMeta, log );
    verify( remarks, times( 2 ) ).add( any( CheckResultInterface.class ) );
  }

  @Test
  public void testCheckOkWithParam() {
    when( mongoDbInputMeta.getJsonQuery() ).thenReturn(
        "{$unwind : 'foo'}, {$match : ${testParam}}" );
    space.setVariable( "testParam", "{foo :'123'}" );

    setupMockedParamGen( "testParam", "testStep" );
    when( stepMeta.getName() )
        .thenReturn( "testStep" );
    mongoDbBasicValidation.checkStep( checkStepsExtension, dataServiceMeta, log );

    verify( remarks, never() ).add( any( CheckResultInterface.class ) );
  }

}
