/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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
import org.eclipse.swt.widgets.Shell;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.serialization.DataServiceAlreadyExistsException;
import org.pentaho.di.trans.dataservice.serialization.SynchronizationListener;
import org.pentaho.di.trans.dataservice.serialization.UndefinedDataServiceException;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingConvertor;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.components.XulMenuList;
import org.pentaho.ui.xul.components.XulMessageBox;
import org.pentaho.ui.xul.components.XulTextbox;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.swt.tags.SwtDialog;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
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

  @Mock SynchronizationListener synchronizationListener;

  @Mock XulDomContainer xulDomContainer;

  @Mock Document document;

  @Mock SwtDialog dialog;

  @Mock Shell shell;

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

    controller = new DataServiceDialogController( model, delegate ) {
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

    when( dialog.getShell() ).thenReturn( shell );

    when( delegate.createSyncService() ).thenReturn( synchronizationListener );
  }

  @Test
  public void testBindings() throws Exception {
    testBindingsAux();
  }

  @Test
  public void testBindingsStreaming() throws Exception {
    when( model.isStreaming() ).thenReturn( true );
    testBindingsAux();
  }

  private void testBindingsAux() throws Exception {
    final BindingFactory bindingFactory = mock( BindingFactory.class );
    final XulTextbox serviceName = mock( XulTextbox.class );
    final XulMenuList<String> steps = mock( XulMenuList.class );
    final XulTextbox maxRows = mock( XulTextbox.class );
    final XulTextbox maxTime = mock( XulTextbox.class );

    when( document.getElementById( "service-name" ) ).thenReturn( serviceName );
    when( document.getElementById( "trans-steps" ) ).thenReturn( steps );
    when( document.getElementById( "streaming-max-rows" ) ).thenReturn( maxRows );
    when( document.getElementById( "streaming-max-time" ) ).thenReturn( maxTime );

    controller.setBindingFactory( bindingFactory );

    final List<Binding> bindings = Lists.newArrayList();
    when( bindingFactory.createBinding( same( model ), anyString(), any( XulComponent.class ), anyString() ) )
      .thenAnswer( invocationOnMock -> {
        Binding binding = mock( Binding.class );
        bindings.add( binding );
        return binding;
      } );
    when( bindingFactory.createBinding( same( model ), anyString(), any( XulComponent.class ), anyString(),
      any( BindingConvertor.class ) ) ).thenAnswer( invocationOnMock -> {
        Binding binding = mock( Binding.class );
        bindings.add( binding );
        return binding;
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
    verify( dialog, never() ).dispose();

    controller.saveAndClose();
    verify( dialog ).dispose();
    verifyNoMoreInteractions( logChannel );
  }

  @Test
  public void testShowTestDialog() throws Exception {
    controller.showTestDialog();

    verify( delegate ).showTestDataServiceDialog( dataServiceMeta, shell );
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
    verify( synchronizationListener ).install( transMeta );
    verify( dialog ).dispose();
  }

  @Test
  public void testSaveAndCloseNoName() throws Exception {
    DataServiceMeta editingDataService = mock( DataServiceMeta.class );
    when( model.getServiceName() ).thenReturn( null );

    controller.saveAndClose();
    verify( delegate, times( 0 ) ).save( dataServiceMeta );
    verify( delegate, times( 0 ) ).removeDataService( editingDataService );
    verify( synchronizationListener, times( 0 ) ).install( transMeta );
    verify( dialog, times( 0 ) ).dispose();
  }

  @Test
  public void testDelegation() throws Exception {
    assertThat( controller.getDialog(), sameInstance( dialog ) );

    controller.open();
    verify( dialog ).show();

    controller.close();
    verify( dialog ).dispose();
  }

  @Test
  public void testShowsErrorOnTestWithNameEmpty() throws XulException {
    DataServiceDialogController controller = spy( new DataServiceDialogController( model, delegate ) );
    doNothing().when( controller ).error( anyString(), anyString() );
    when( model.getServiceName() ).thenReturn( null );
    controller.showTestDialog();
    verify( controller, times( 1 ) ).error( anyString(), anyString() );
  }

  @Test
  public void testShowDriverDetails() {
    DataServiceDelegate delegate = mock( DataServiceDelegate.class );
    controller = spy( new DataServiceDialogController( null, delegate ) );
    SwtDialog dialog = mock( SwtDialog.class );
    doReturn( dialog ).when( controller ).getDialog();
    Shell shell = mock( Shell.class );
    doReturn( shell ).when( dialog ).getShell();
    doCallRealMethod().when( controller ).showDriverDetailsDialog();

    controller.showDriverDetailsDialog();

    verify( delegate ).showDriverDetailsDialog( shell );
  }

  @Test
  public void testShowHelpUsesDialogShell() throws Exception {
    controller = mock( DataServiceDialogController.class );
    doCallRealMethod().when( controller ).showHelp();
    SwtDialog dialog = mock( SwtDialog.class );
    doReturn( dialog ).when( controller ).getDialog();
    IllegalStateException runtimeException = new IllegalStateException();
    doThrow( runtimeException ).when( dialog ).getShell();

    try {
      controller.showHelp();
    } catch ( Exception e ) {
      MatcherAssert.assertThat( runtimeException, is( sameInstance( e ) ) );
    }

    verify( dialog ).getShell();
  }

}
