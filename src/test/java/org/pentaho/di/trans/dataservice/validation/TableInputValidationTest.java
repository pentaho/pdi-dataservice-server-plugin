/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class TableInputValidationTest extends BaseStepValidationTest {

  private TableInputMeta tableInputMeta;

  private TableInputValidation tableInputValidation = new TableInputValidation();

  @Override
  public void init() {
    tableInputMeta = mock( TableInputMeta.class );
    when( stepMeta.getStepMetaInterface() ).thenReturn( tableInputMeta );
  }

  @Test
  public void testIsApplicable() {
    when( stepMeta.getStepMetaInterface() ).thenReturn( tableInputMeta );
    assertTrue( tableInputValidation.supportsStep( stepMeta, log ) );
  }

  @Test
  public void testCheckNoParamSQLisNull() {
    tableInputValidation.checkStep( checkStepsExtension, dataServiceMeta, log );

    // make sure something was added to remarks
    verify( remarks, times( 1 ) ).add( any( CheckResultInterface.class ) );
  }

  @Test
  public void testCheckNoParam() {
    when( tableInputMeta.getSQL() ).thenReturn(
        "SELECT foo FROM bar" );
    tableInputValidation.checkStep( checkStepsExtension, dataServiceMeta, log );

    verify( remarks, times( 1 ) ).add( any( CheckResultInterface.class ) );
  }

  @Test
  public void testCheckNoWarningsWithParam() {
    when( tableInputMeta.getSQL() ).thenReturn(
        "SELECT foo FROM bar WHERE ${baz}" );
    space.setVariable( "baz", "foo='123'" );
    tableInputValidation.checkStep( checkStepsExtension, dataServiceMeta, log );

    verify( remarks, never() ).add( any( CheckResultInterface.class ) );
  }

  @Test
  public void testCheckNoParamForDefinedOpt() {
    when( tableInputMeta.getSQL() ).thenReturn(
        "SELECT foo FROM bar" );

    setupMockedParamGen( "testParam", "testStep" );
    when( stepMeta.getName() )
        .thenReturn( "testStep" );
    tableInputValidation.checkStep( checkStepsExtension, dataServiceMeta, log );

    verify( remarks, times( 2 ) ).add( any( CheckResultInterface.class ) );
  }

  @Test
  public void testCheckOkWithParam() {
    when( tableInputMeta.getSQL() ).thenReturn(
        "SELECT foo FROM bar ${testParam}" );
    space.setVariable( "testParam", "foo='123'" );

    setupMockedParamGen( "testParam", "testStep" );
    when( stepMeta.getName() )
        .thenReturn( "testStep" );
    tableInputValidation.checkStep( checkStepsExtension, dataServiceMeta, log );

    verify( remarks, never() ).add( any( CheckResultInterface.class ) );
  }

}
