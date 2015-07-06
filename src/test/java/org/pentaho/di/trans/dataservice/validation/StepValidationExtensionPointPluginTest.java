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

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.pentaho.di.trans.CheckStepsExtension;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMetaStoreUtil;
import org.pentaho.di.trans.step.StepMeta;

import static org.mockito.Matchers.any;
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
    when( metaStoreUtil.fromTransMeta( transMeta, metaStore, null ) ).thenReturn( dataServiceMeta );
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
