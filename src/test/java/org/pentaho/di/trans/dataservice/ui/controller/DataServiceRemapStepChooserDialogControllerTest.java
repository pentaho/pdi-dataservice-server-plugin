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

package org.pentaho.di.trans.dataservice.ui.controller;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.dataservice.ui.DataServiceRemapStepChooserDialog;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceRemapStepChooserModel;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.swt.tags.SwtDialog;
import org.pentaho.ui.xul.swt.tags.SwtLabel;
import org.pentaho.ui.xul.swt.tags.SwtListbox;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataServiceRemapStepChooserDialogControllerTest {
  @Test
  public void testInit() throws InvocationTargetException, XulException {
    DataServiceRemapStepChooserModel model = mock( DataServiceRemapStepChooserModel.class );
    DataServiceMeta dataService = mock( DataServiceMeta.class );
    List<String> stepNames = Arrays.asList( "step1" );
    DataServiceDelegate delegate = mock( DataServiceDelegate.class );
    final SwtLabel label = mock( SwtLabel.class );
    final SwtListbox listbox = mock( SwtListbox.class );
    BindingFactory bindingFactory = mock( BindingFactory.class );
    when( bindingFactory.createBinding( same( model ), anyString(), any( XulComponent.class ), anyString() ) )
        .thenReturn( mock( Binding.class ) );

    DataServiceRemapStepChooserDialogController
        controller =
        spy( new DataServiceRemapStepChooserDialogController( model, dataService, stepNames, delegate ) );
    doAnswer( new Answer() {
      private Object[] returnValues = { label, listbox };
      int invocations = 0;

      @Override public Object answer( InvocationOnMock invocationOnMock ) throws Throwable {
        return returnValues[invocations++];
      }
    } ).when( controller ).getElementById( anyString() );
    doReturn( bindingFactory ).when( controller ).getBindingFactory();

    controller.init();

    verify( label ).setValue( anyString() );
    verify( listbox ).setElements( same( stepNames ) );
    verify( bindingFactory ).createBinding( same( model ), anyString(), same( listbox ), anyString() );
  }

  @Test
  public void testRemap() throws Exception {
    DataServiceRemapStepChooserModel model = mock( DataServiceRemapStepChooserModel.class );
    DataServiceMeta dataService = mock( DataServiceMeta.class );
    List<String> stepNames = Arrays.asList( "step1", "step2" );
    DataServiceDelegate delegate = mock( DataServiceDelegate.class );
    SwtDialog dialog = mock( SwtDialog.class );

    when( model.getServiceStep() ).thenReturn( "step2" );

    doNothing().doThrow( Exception.class ).when( delegate ).save( any( DataServiceMeta.class ) );

    DataServiceRemapStepChooserDialogController
        controller =
        spy( new DataServiceRemapStepChooserDialogController( model, dataService, stepNames, delegate ) );
    doReturn( dialog ).when( controller ).getDialog();

    controller.remap();

    verify( dataService ).setStepname( "step2" );
    verify( delegate ).save( same( dataService ) );
    Assert.assertEquals( DataServiceRemapStepChooserDialog.Action.REMAP, controller.getAction() );

    controller.remap();
    verify( delegate ).showError( anyString(), anyString() );
    Assert.assertEquals( DataServiceRemapStepChooserDialog.Action.CANCEL, controller.getAction() );
  }
}
