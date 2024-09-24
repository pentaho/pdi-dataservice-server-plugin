/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.optimization.paramgen.ui;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGenerationFactory;
import org.pentaho.di.trans.dataservice.ui.DataServiceDialog;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.ui.xul.XulDomContainer;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class ParameterGenerationOverlayTest {

  @Mock ParameterGenerationFactory factory;
  @Mock DataServiceDialog dialog;
  @Mock DataServiceModel model;
  @InjectMocks ParameterGenerationOverlay overlay;

  @Test
  public void testApply() throws Exception {
    ParameterGenerationController controller = mock( ParameterGenerationController.class );
    XulDomContainer xulDomContainer = mock( XulDomContainer.class );
    TransMeta transMeta = mock( TransMeta.class );
    StepMeta supportedStep = new StepMeta( "supportedStep", mock( StepMetaInterface.class ) );

    when( dialog.getModel() ).thenReturn( model );
    when( model.getTransMeta() ).thenReturn( transMeta );
    when( transMeta.getSteps() ).thenReturn( ImmutableList.of( supportedStep, mock( StepMeta.class ) ) );
    when( factory.supportsStep( supportedStep ) ).thenReturn( true );
    when( factory.createController( model ) ).thenReturn( controller );
    when( dialog.applyOverlay( same( overlay ), anyString() ) ).thenReturn( xulDomContainer );

    overlay.apply( dialog );

    verify( xulDomContainer ).addEventHandler( controller );
    verify( controller ).initBindings( ImmutableList.of( "supportedStep" ) );
  }
}
