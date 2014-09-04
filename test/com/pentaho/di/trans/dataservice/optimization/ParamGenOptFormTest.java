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

package com.pentaho.di.trans.dataservice.optimization;

import com.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGeneration;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.junit.Test;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.ui.core.PropsUI;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/*
 * Note that this test class depends on the swt Shell.
 * If you get an UnsatisfiedLinkError when running this test from ant it may
 * indicate that your devlib.dir setting in build.properties is not pointed to
 * the correct swt lib path for your platform.
 */
public class ParamGenOptFormTest extends ParamGenOptForm {

  @Test
  public void testApplyOptimizationParameters() throws Exception {
    PushDownOptimizationMeta optMeta = new PushDownOptimizationMeta();
    optMeta.setName( "testOptName" );
    optMeta.setStepName( "testTableStep" );
    populateForm( new Shell(), makeTestPropsUI(),
      makeTestTransMeta(), optMeta );

    paramNameText.setText( "testParamName" );
    applyOptimizationParameters( optMeta );

    ParameterGeneration paramGen = (ParameterGeneration) optMeta.getType();

    assertEquals( "No mappings were added", 0, paramGen.getFieldMappings().size() );
    assertEquals( "testParamName", paramGen.getParameterName() );
  }

  @Test
  public void testApplyOptWithInitialMappings() {
    PushDownOptimizationMeta optMeta = new PushDownOptimizationMeta();
    optMeta.setName( "testOptName" );
    optMeta.setStepName( "testTableStep" );
    ParameterGeneration paramGen = new ParameterGeneration();
    paramGen.createFieldMapping( "source0", "target0" );
    paramGen.createFieldMapping( "source1", "target1" );
    optMeta.setType( paramGen );
    populateForm( new Shell(), makeTestPropsUI(),
      makeTestTransMeta(), optMeta );

    // add one
    TableItem item = new TableItem( definitionTable.table, SWT.NONE );
    item.setText( 1, "target2" );
    item.setText( 2, "source2" );

    applyOptimizationParameters( optMeta );
    ParameterGeneration updatedParamGen = (ParameterGeneration)optMeta.getType();
    assertEquals( "Should be 3 mappings", 3, updatedParamGen.getFieldMappings().size() );
    for ( int i = 0; i < 3; i++ ) {
      SourceTargetFields srcTarget = updatedParamGen.getFieldMappings().get( i );
      assertEquals( "source" + i, srcTarget.getSourceFieldName() );
      assertEquals( "target" + i, srcTarget.getTargetFieldName() );
    }
  }

  private PropsUI makeTestPropsUI() {
    PropsUI props = mock( PropsUI.class );
    when( props.getGridFont() ).thenReturn( new FontData() );
    return props;
  }

  private TransMeta makeTestTransMeta() {
    TransMeta transMeta = mock( TransMeta.class );
    StepMeta tableStep = mock( StepMeta.class );
    StepMeta otherStep = mock( StepMeta.class );
    StepMetaInterface stepMetaInterface = mock( StepMetaInterface.class );
    DatabaseMeta databaseMeta = mock( DatabaseMeta.class );
    when( stepMetaInterface.getUsedDatabaseConnections())
      .thenReturn( new DatabaseMeta[] { databaseMeta } );
    when( tableStep.getStepMetaInterface()).thenReturn( stepMetaInterface );
    when( tableStep.getName() ).thenReturn( "testTableStep" );
    when( tableStep.getTypeId() ).thenReturn( "TableInput" );
    when( transMeta.getSteps() )
    .thenReturn( Arrays.asList( tableStep, otherStep ) );
    return transMeta;
  }
}
