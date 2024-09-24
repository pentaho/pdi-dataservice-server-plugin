/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.di.trans.dataservice.validation;

import org.junit.Test;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
