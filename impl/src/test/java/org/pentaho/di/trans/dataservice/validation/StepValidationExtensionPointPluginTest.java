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

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.pentaho.di.trans.CheckStepsExtension;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.trans.step.StepMeta;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StepValidationExtensionPointPluginTest extends BaseStepValidationTest {

  private CheckStepsExtension spiedCheckStepExtension;
  @Mock private DataServiceContext context;
  @Mock private DataServiceMetaStoreUtil metaStoreUtil;
  @Mock private StepValidation stepValidation;
  private StepValidationExtensionPointPlugin extensionPointPlugin;

  @Before
  public void setUp() throws Exception {
    spiedCheckStepExtension = spy( checkStepsExtension );

    when( context.getMetaStoreUtil() ).thenReturn( metaStoreUtil );
    when( metaStoreUtil.getDataServiceByStepName( transMeta, null ) ).thenReturn( dataServiceMeta );
    extensionPointPlugin = new StepValidationExtensionPointPlugin( context );
    extensionPointPlugin.setStepValidations( ImmutableList.of( stepValidation ) );
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
}
