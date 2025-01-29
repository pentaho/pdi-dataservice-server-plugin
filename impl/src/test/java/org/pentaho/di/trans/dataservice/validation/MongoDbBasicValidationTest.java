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


package org.pentaho.di.trans.dataservice.validation;

import org.junit.Test;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
