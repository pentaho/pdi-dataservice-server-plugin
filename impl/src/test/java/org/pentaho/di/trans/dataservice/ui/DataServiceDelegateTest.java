/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.dataservice.ui;

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.dataservice.BaseTest;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.serialization.SynchronizationListener;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.ui.xul.XulException;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataServiceDelegateTest extends BaseTest {

  private static final String STEP_NAME = "Step Name";
  private static final String TITLE = "Test Title";
  private static final String TEXT = "Test Text";

  @Mock
  private Spoon spoon;

  @Mock
  private DataServiceDialog dataServiceDialog;

  @Mock
  private DataServiceTestDialog dataServiceTestDialog;

  @Mock
  private Shell shell;

  @Mock
  private MessageBox messageBox;

  @Mock DataServiceDialog.Builder dialogBuilder;

  @Mock
  private MessageDialog messageDialog;


  @Before
  public void setUp() throws Exception {
    when( spoon.getActiveTransformation() ).thenReturn( transMeta );
    when( spoon.getShell() ).thenReturn( shell );
    when( uiFactory.getMessageBox( any( Shell.class ), anyInt() ) ).thenReturn( messageBox );
    when( uiFactory.getDataServiceTestDialog( any( Shell.class ), any( DataServiceMeta.class ), any(
      DataServiceContext.class ) ) )
      .thenReturn( dataServiceTestDialog );
    when( uiFactory.getMessageDialog( any(), anyString(), any(), anyString(), anyInt(), any(), anyInt() ) )
      .thenReturn( messageDialog );
    when( messageDialog.open() ).thenReturn( 0 );

    delegate = new DataServiceDelegate( context, spoon );

    when( uiFactory.getDataServiceDialogBuilder( transMeta ) ).thenReturn( dialogBuilder );
    when( dialogBuilder.serviceStep( STEP_NAME ) ).thenReturn( dialogBuilder );
    when( dialogBuilder.edit( dataService ) ).thenReturn( dialogBuilder );
    when( dialogBuilder.build( delegate ) ).thenReturn( dataServiceDialog );
  }

  @Test
  public void testDefaultSpoonFactoryMethod() throws Exception {
    assertThat( DataServiceDelegate.withDefaultSpoonInstance( context ), notNullValue() );
  }


  @Test
  public void testCreateNewDataService() throws Exception {
    delegate.createNewDataService( STEP_NAME );

    verify( dialogBuilder ).serviceStep( STEP_NAME );
    verify( dataServiceDialog ).open();
  }

  @Test
  public void testCreateNewDataServiceNoServiceStepProvided() throws KettleException {
    delegate = spy( delegate );

    when( dialogBuilder.serviceStep( anyString() ) ).thenReturn( dialogBuilder );
    when( dialogBuilder.build( delegate ) ).thenReturn( dataServiceDialog );

    delegate.createNewDataService( null );

    verify( delegate ).getLastStepOfActiveTrans();
  }

  @Test
  public void testEditDataService() throws Exception {
    delegate.editDataService( dataService );

    verify( dialogBuilder ).edit( dataService );
    verify( dataServiceDialog ).open();
  }

  @Test
  public void testSuggestEdit() throws Exception {
    when( messageBox.open() ).thenReturn( SWT.YES );

    delegate.suggestEdit( dataService, TITLE, TEXT );

    assertThat( transMeta.hasChanged(), is( true ) );
  }

  @Test
  public void testSuggestEditNo() throws Exception {
    delegate = spy( delegate );
    when( messageBox.open() ).thenReturn( SWT.NO );
    delegate.suggestEdit( dataService, TITLE, TEXT );

    Mockito.verify( delegate, Mockito.times( 1 ) ).
      deleteDataServiceElementAndCleanCache( dataService, transMeta );
  }

  @Test
  public void testShowError() throws Exception {
    delegate.showError( TITLE, TEXT );

    verify( messageBox ).setText( TITLE );
    verify( messageBox ).setMessage( TEXT );
    verify( messageBox ).open();
  }

  @Test
  public void testShowPrompt() throws Exception {
    when( messageBox.open() ).thenReturn( SWT.YES );

    boolean open = delegate.showPrompt( TITLE, TEXT );

    assertThat( open, is( true ) );

    verify( messageBox ).setText( TITLE );
    verify( messageBox ).setMessage( TEXT );
    verify( messageBox ).open();
  }

  @Test
  public void testRemoveDataService() throws Exception {
    when( messageBox.open() ).thenReturn( SWT.YES );
    delegate.removeDataService( dataService, true );
  }

  @Test
  public void testTestDataService() throws Exception {
    delegate.showTestDataServiceDialog( dataService );

    verify( uiFactory ).getDataServiceTestDialog( shell, dataService, context );
  }

  @Test
  public void testShowSavePrompt() throws Exception {
    transMeta.setChanged();

    when( messageDialog.open() ).thenReturn( 0 );
    when( spoon.saveToFile( transMeta ) ).then( new Answer<Boolean>() {
      @Override public Boolean answer( InvocationOnMock invocation ) throws Throwable {
        transMeta.clearChanged();
        return true;
      }
    } );

    delegate.createNewDataService( STEP_NAME );

    verify( spoon ).saveToFile( transMeta );
    verify( dialogBuilder ).serviceStep( STEP_NAME );
    verify( dataServiceDialog ).open();
  }

  @Test
  public void testSyncService() throws Exception {
    assertThat( delegate.createSyncService(), instanceOf( SynchronizationListener.class ) );
  }

  @Test
  public void testSave() throws Exception {
    assertThat( delegate.getDataServiceNames( transMeta ), is( empty() ) );
    delegate.save( dataService );
    assertThat( delegate.getDataServiceNames( transMeta ), contains( DATA_SERVICE_NAME ) );
  }

  @Test
  public void testShowDriverDetailsDialog() throws KettleException, XulException {
    DriverDetailsDialog dialog = mock( DriverDetailsDialog.class );
    doReturn( dialog ).when( uiFactory ).getDriverDetailsDialog( shell );

    delegate.showDriverDetailsDialog();

    verify( dialog ).open();

    Exception e = new RuntimeException();
    doThrow( e ).when( dialog ).open();

    delegate.showDriverDetailsDialog();

    verify( logChannel ).logError( anyString(), same( e ) );
  }

  @Test
  public void testTransStepIsReturnedNoCurrentStepExists() {
    StepMeta step = delegate.getLastStepOfActiveTrans();

    assertEquals( DATA_SERVICE_STEP, step.getName() );
  }

  @Test
  public void testShowRemapConfirmationDialog() throws KettleException {
    DataServiceRemapConfirmationDialog dialog = mock( DataServiceRemapConfirmationDialog.class );

    when( dialog.getAction() ).thenReturn( DataServiceRemapConfirmationDialog.Action.REMAP );
    when( uiFactory.getRemapConfirmationDialog( any(), any(), any(), any() ) ).thenReturn( dialog );

    assertThat( delegate.showRemapConfirmationDialog( null, null ),
        is( DataServiceRemapConfirmationDialog.Action.REMAP ) );
    verify( dialog ).open();

    KettleException e = new KettleException();
    doThrow( e ).when( dialog ).open();

    assertThat( delegate.showRemapConfirmationDialog( null, null ),
        is( DataServiceRemapConfirmationDialog.Action.CANCEL ) );
    verify( logChannel ).logError( anyString(), same( e ) );
  }

  @Test
  public void testShowRemapStepChooserDialog() throws KettleException {
    DataServiceRemapStepChooserDialog dialog = mock( DataServiceRemapStepChooserDialog.class );

    when( dialog.getAction() ).thenReturn( DataServiceRemapStepChooserDialog.Action.REMAP );
    when( uiFactory.getRemapStepChooserDialog( any(), any(), any(), any() ) ).thenReturn( dialog );

    assertThat( delegate.showRemapStepChooserDialog( null, null, null ),
        is( DataServiceRemapStepChooserDialog.Action.REMAP ) );
    verify( dialog ).open();

    KettleException e = new KettleException();
    doThrow( e ).when( dialog ).open();

    assertThat( delegate.showRemapStepChooserDialog( null, null, null ),
        is( DataServiceRemapStepChooserDialog.Action.CANCEL ) );
    verify( logChannel ).logError( anyString(), same( e ) );
  }

  @Test
  public void testShowRemapNoStepsDialog() throws KettleException {
    DataServiceRemapNoStepsDialog dialog = mock( DataServiceRemapNoStepsDialog.class );

    when( uiFactory.getRemapNoStepsDialog( any() ) ).thenReturn( dialog );

    delegate.showRemapNoStepsDialog( null );
    verify( dialog ).open();

    KettleException e = new KettleException();
    doThrow( e ).when( dialog ).open();

    delegate.showRemapNoStepsDialog( null );
    verify( logChannel ).logError( anyString(), same( e ) );
  }
}
