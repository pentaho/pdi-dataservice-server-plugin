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

import com.google.common.collect.ImmutableList;
import com.pentaho.di.trans.dataservice.DataServiceMetaStoreUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.pentaho.di.trans.CheckStepsExtension;
import org.pentaho.di.trans.step.StepMeta;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StepValidationExtensionPointPluginTest extends BaseStepValidationTest {

  private CheckStepsExtension spiedCheckStepExtension;
  @Mock private DataServiceMetaStoreUtil metaStoreUtil;
  @Mock private StepValidation stepValidation;
  @InjectMocks private StepValidationExtensionPointPlugin extensionPointPlugin;

  @Before
  public void setUp() throws Exception {
    spiedCheckStepExtension = spy( checkStepsExtension );
    extensionPointPlugin.setStepValidations( ImmutableList.of( stepValidation ) );
    when( metaStoreUtil.fromTransMeta( transMeta, metaStore, null ) ).thenReturn( dataServiceMeta );
  }

  @Test
  public void testCallStepValidations() throws Exception {
    when( stepValidation.supportsStep( stepMeta, log ) ).thenReturn( true, false );

    extensionPointPlugin.callExtensionPoint( log, spiedCheckStepExtension );
    extensionPointPlugin.callExtensionPoint( log, spiedCheckStepExtension );

    verify( stepValidation ).checkStep( spiedCheckStepExtension, dataServiceMeta, log );
  }

  @Test
  public void testCallExtensionPointInvalidObjType() throws Exception {
    extensionPointPlugin.callExtensionPoint( log, "FooBar" );
    //make sure we logged an error
    verify( log ).logError( any( String.class ) );
  }

  @Test
  public void testCallExtensionPointWrongNumSteps() throws Exception {
    when( spiedCheckStepExtension.getStepMetas() )
        .thenReturn( new StepMeta[ 5 ] );

    extensionPointPlugin.callExtensionPoint( log, spiedCheckStepExtension );
    //make sure we logged an error
    verify( log ).logError( any( String.class ) );
  }

  @Test
  public void testCallExtensionPointDataServiceLoadIssue() throws Exception {
    when( stepValidation.supportsStep( stepMeta, log ) ).thenReturn( true, false );
    when( spiedCheckStepExtension.getMetaStore() )
        .thenReturn( null );

    extensionPointPlugin.callExtensionPoint( log, spiedCheckStepExtension );
    //make sure we logged a message
    verify( log ).logBasic( any( String.class ) );
  }

}
