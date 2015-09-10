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

package org.pentaho.di.trans.dataservice.ui.controller;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.serialization.DataServiceAlreadyExistsException;
import org.pentaho.di.trans.dataservice.serialization.UndefinedDataServiceException;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.components.XulMenuList;
import org.pentaho.ui.xul.components.XulMessageBox;
import org.pentaho.ui.xul.components.XulTextbox;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.dom.Document;

import java.util.List;

import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class DataServiceDialogControllerTest {

  @Mock TransMeta transMeta;

  @Mock DataServiceModel model;

  @Mock DataServiceMeta dataServiceMeta;

  @Mock DataServiceDelegate delegate;

  @Mock XulDomContainer xulDomContainer;

  @Mock Document document;

  @Mock XulDialog dialog = mock( XulDialog.class );

  @Mock XulMessageBox messageBox;

  @Mock LogChannelInterface logChannel;

  private static final String SERVICE_NAME = "test_service";
  private static final String EDITING_SERVICE_NAME = "test_service2";
  private static final String SELECTED_STEP = "Output Step";
  private static final String STEP_ONE_NAME = "Step One";
  private static final String STEP_TWO_NAME = "Step Two";

  private DataServiceDialogController controller;

  @Before
  public void init() throws Exception {
    when( xulDomContainer.getDocumentRoot() ).thenReturn( document );
    when( document.getElementById( DataServiceDialogController.XUL_DIALOG_ID ) ).thenReturn( dialog );
    when( document.createElement( "messagebox" ) ).thenReturn( messageBox );

    controller = new DataServiceDialogController( model, delegate ){
      @Override protected LogChannelInterface getLogChannel() {
        return logChannel;
      }
    };
    controller.setXulDomContainer( xulDomContainer );

    doReturn( SERVICE_NAME ).when( dataServiceMeta ).getName();
    doReturn( new String[] { STEP_ONE_NAME, STEP_TWO_NAME } ).when( transMeta ).getStepNames();

    when( model.getServiceName() ).thenReturn( SERVICE_NAME );
    when( model.getServiceStep() ).thenReturn( SELECTED_STEP );
    when( model.getTransMeta() ).thenReturn( transMeta );

    when( dataServiceMeta.getName() ).thenReturn( SERVICE_NAME );
    when( dataServiceMeta.getStepname() ).thenReturn( SELECTED_STEP );

    when( model.getDataService() ).thenReturn( dataServiceMeta );
  }

  @Test
  @SuppressWarnings( "unchecked" )
  public void testBindings() throws Exception {
    final BindingFactory bindingFactory = mock( BindingFactory.class );
    final XulTextbox serviceName = mock( XulTextbox.class );
    final XulMenuList<String> steps = mock( XulMenuList.class );

    when( document.getElementById( "service-name" ) ).thenReturn( serviceName );
    when( document.getElementById( "trans-steps" ) ).thenReturn( steps );

    controller.setBindingFactory( bindingFactory );

    final List<Binding> bindings = Lists.newArrayList();
    when( bindingFactory.createBinding( same( model ), anyString(), anyObject(), anyString() ) ).thenAnswer(
      new Answer<Binding>() {
        @Override public Binding answer( InvocationOnMock invocation ) throws Throwable {
          Binding binding = mock( Binding.class );
          bindings.add( binding );
          return binding;
        }
      } );

    controller.init();

    verify( steps ).setElements( eq( ImmutableList.of( STEP_ONE_NAME, STEP_TWO_NAME ) ) );
    for ( Binding binding : bindings ) {
      verify( binding ).fireSourceChanged();
    }
  }

  @Test
  public void testError() throws Exception {
    UndefinedDataServiceException undefinedException = new UndefinedDataServiceException( dataServiceMeta );
    doThrow( undefinedException )
      .doReturn( dataServiceMeta )
      .when( delegate ).checkDefined( dataServiceMeta );

    DataServiceAlreadyExistsException alreadyExistsException = new DataServiceAlreadyExistsException( dataServiceMeta );
    doThrow( alreadyExistsException )
      .doReturn( dataServiceMeta )
      .when( delegate ).checkConflict( dataServiceMeta, null );

    MetaStoreException metaStoreException = new MetaStoreException();
    doThrow( metaStoreException ).doNothing().when( delegate ).save( dataServiceMeta );

    controller.saveAndClose();
    verify( messageBox ).setMessage( undefinedException.getMessage() );

    controller.saveAndClose();
    verify( messageBox ).setMessage( alreadyExistsException.getMessage() );

    verify( delegate, never() ).save( any( DataServiceMeta.class ) );

    controller.saveAndClose();

    verify( messageBox, times( 3 ) ).open();
    verify( logChannel ).logError( anyString(), same( metaStoreException ) );
    verify( dialog, never() ).hide();

    controller.saveAndClose();
    verify( dialog ).hide();
    verifyNoMoreInteractions( logChannel );
  }

  @Test
  public void testShowTestDialog() throws Exception {
    controller = new DataServiceDialogController( model, delegate );

    controller.showTestDialog();

    verify( delegate ).testDataService( dataServiceMeta );
  }

  @Test
  public void testSaveAndClose() throws Exception {
    DataServiceMeta editingDataService = mock( DataServiceMeta.class );
    controller.setDataService( editingDataService );
    when( editingDataService.getName() ).thenReturn( EDITING_SERVICE_NAME );

    when( delegate.checkDefined( dataServiceMeta ) ).thenReturn( dataServiceMeta );
    when( delegate.checkConflict( dataServiceMeta, EDITING_SERVICE_NAME ) ).thenReturn( dataServiceMeta );

    controller.saveAndClose();
    verify( delegate ).save( dataServiceMeta );
    verify( delegate ).removeDataService( editingDataService );
    verify( dialog ).hide();
  }

  @Test
  public void testDelegation() throws Exception {
    assertThat( controller.getDialog(), sameInstance( dialog ) );

    controller.open();
    verify( dialog ).show();

    controller.close();
    verify( dialog ).hide();
  }
}
