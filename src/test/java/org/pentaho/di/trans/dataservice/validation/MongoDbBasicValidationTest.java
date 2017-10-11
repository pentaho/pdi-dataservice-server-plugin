/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.dataservice.validation;

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
