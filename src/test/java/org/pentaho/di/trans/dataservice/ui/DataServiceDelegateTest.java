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

package org.pentaho.di.trans.dataservice.ui;

/**
 * Created by bmorrise on 10/16/15.
 */

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.trans.dataservice.BaseTest;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.serialization.SynchronizationService;
import org.pentaho.di.ui.spoon.Spoon;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
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

  @Test
  public void testDefaultSpoonFactoryMethod() throws Exception {
    assertThat( DataServiceDelegate.withDefaultSpoonInstance( context ), notNullValue() );
  }

  @Before
  public void setUp() throws Exception {
    when( spoon.getActiveTransformation() ).thenReturn( transMeta );
    when( spoon.getShell() ).thenReturn( shell );
    when( uiFactory.getMessageBox( any( Shell.class ), anyInt() ) ).thenReturn( messageBox );
    when( uiFactory.getShell( shell ) ).thenReturn( shell );
    when( uiFactory.getDataServiceTestDialog( any( Shell.class ), any( DataServiceMeta.class ) ) )
        .thenReturn( dataServiceTestDialog );
    when( uiFactory
        .getMessageDialog( any( Shell.class ), anyString(), any( Image.class ), anyString(), anyInt(), any( String[].class ),
            anyInt() ) ).thenReturn( messageDialog );
    when( messageDialog.open() ).thenReturn( 0 );

    delegate = new DataServiceDelegate( context, spoon );

    when( uiFactory.getDataServiceDialogBuilder( transMeta ) ).thenReturn( dialogBuilder );
    when( dialogBuilder.serviceStep( STEP_NAME ) ).thenReturn( dialogBuilder );
    when( dialogBuilder.edit( dataService ) ).thenReturn( dialogBuilder );
    when( dialogBuilder.build( delegate ) ).thenReturn( dataServiceDialog );
  }

  @Test
  public void testCreateNewDataService() throws Exception {
    delegate.createNewDataService( STEP_NAME );

    verify( dialogBuilder ).serviceStep( STEP_NAME );
    verify( dataServiceDialog ).open();
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
    delegate.testDataService( dataService );

    verify( uiFactory ).getDataServiceTestDialog( shell, dataService );
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
    assertThat( delegate.createSyncService(), instanceOf( SynchronizationService.class ) );
  }

  @Test
  public void testSave() throws Exception {
    assertThat( delegate.getDataServiceNames( transMeta ), is( empty() ) );
    delegate.save( dataService );
    assertThat( delegate.getDataServiceNames( transMeta ), contains( DATA_SERVICE_NAME ) );
  }

}
