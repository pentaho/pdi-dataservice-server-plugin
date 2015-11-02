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
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metastore.api.IMetaStore;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class DataServiceDelegateTest {

  private static final String STEP_NAME = "Step Name";
  private static final String TITLE = "Test Title";
  private static final String TEXT = "Test Text";
  private static final String DATA_SERVICE_NAME = "Data Service Name";

  @Mock
  private DataServiceContext context;

  @Mock
  private TransMeta transMeta;

  @Mock
  private Spoon spoon;

  @Mock
  private UIFactory uiFactory;

  @Mock
  private DataServiceDialog.Builder builder;

  @Mock
  private DataServiceDialog dataServiceDialog;

  @Mock
  private DataServiceTestDialog dataServiceTestDialog;

  @Mock
  private DataServiceMeta dataServiceMeta;

  @Mock
  private Shell shell;

  @Mock
  private MessageBox messageBox;

  @Mock
  private MessageDialog messageDialog;

  private DataServiceDelegate delegate;

  @Before
  public void setUp() throws Exception {
    when( dataServiceMeta.getName() ).thenReturn( DATA_SERVICE_NAME );
    when( context.getMetaStoreUtil() ).thenReturn( new DataServiceMetaStoreUtil( context, null ) );
    when( spoon.getActiveTransformation() ).thenReturn( transMeta );
    when( spoon.getShell() ).thenReturn( shell );
    when( context.getUIFactory() ).thenReturn( uiFactory );
    when( dataServiceMeta.getServiceTrans() ).thenReturn( transMeta );
    when( uiFactory.getMessageBox( any( Shell.class ), anyInt() ) ).thenReturn( messageBox );
    when( uiFactory.getShell( shell ) ).thenReturn( shell );
    when( uiFactory.getDataServiceTestDialog( any( Shell.class ), any( DataServiceMeta.class ) ) )
        .thenReturn( dataServiceTestDialog );
    when( uiFactory
        .getMessageDialog( any( Shell.class ), anyString(), any( Image.class ), anyString(), anyInt(), any( String[].class ),
            anyInt() ) ).thenReturn( messageDialog );
    when( messageDialog.open() ).thenReturn( 0 );

    delegate = new DataServiceDelegate( context, spoon );
  }

  @Test
  public void testCreateNewDataService() throws Exception {
    when( transMeta.hasChanged() ).thenReturn( false );
    when( uiFactory.getDataServiceDialogBuilder( transMeta ) ).thenReturn( builder );
    when( builder.serviceStep( STEP_NAME ) ).thenReturn( builder );
    when( builder.build( delegate ) ).thenReturn( dataServiceDialog );

    delegate.createNewDataService( STEP_NAME );

    verify( transMeta ).hasChanged();
    verify( context ).getUIFactory();
    verify( uiFactory ).getDataServiceDialogBuilder( transMeta );
    verify( builder ).serviceStep( STEP_NAME );
    verify( builder ).build( delegate );
    verify( dataServiceDialog ).open();
  }

  @Test
  public void testEditDataService() throws Exception {
    when( transMeta.hasChanged() ).thenReturn( false );
    when( uiFactory.getDataServiceDialogBuilder( transMeta ) ).thenReturn( builder );
    when( builder.edit( dataServiceMeta ) ).thenReturn( builder );
    when( builder.build( delegate ) ).thenReturn( dataServiceDialog );

    delegate.editDataService( dataServiceMeta );

    verify( transMeta ).hasChanged();
    verify( context ).getUIFactory();
    verify( uiFactory ).getDataServiceDialogBuilder( transMeta );
    verify( builder ).edit( dataServiceMeta );
    verify( builder ).build( delegate );
    verify( dataServiceDialog ).open();
  }

  @Test
  public void testSuggestEdit() throws Exception {
    when( uiFactory.getDataServiceDialogBuilder( transMeta ) ).thenReturn( builder );
    when( builder.edit( dataServiceMeta ) ).thenReturn( builder );
    when( builder.build( delegate ) ).thenReturn( dataServiceDialog );
    when( messageBox.open() ).thenReturn( SWT.YES );
    when( transMeta.hasChanged() ).thenReturn( false );

    delegate.suggestEdit( dataServiceMeta, TITLE, TEXT );

    verify( dataServiceMeta, times( 3 ) ).getServiceTrans();
    verify( transMeta, times( 2 ) ).hasChanged();
    verify( transMeta ).setChanged();
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

    DataServiceDelegate spyDelegate = spy( delegate );
    doNothing().when( spyDelegate ).removeDataService( dataServiceMeta );

    spyDelegate.removeDataService( dataServiceMeta, true );
  }

  @Test
  public void testTestDataService() throws Exception {
    delegate.testDataService( dataServiceMeta );

    verify( context, times( 2 ) ).getUIFactory();
    verify( uiFactory ).getDataServiceTestDialog( shell, dataServiceMeta );
  }

  @Test
  public void testShowSavePrompt() throws Exception {
    when( transMeta.hasChanged() ).thenReturn( true );

    delegate.createNewDataService( STEP_NAME );

    verify( context ).getUIFactory();
    verify( messageDialog ).open();
    verify( spoon ).saveToFile( transMeta );
  }

  @Test
  public void testSpoonDelegation(){
    Repository repository = mock( Repository.class );
    IMetaStore metaStore = mock( IMetaStore.class );

    when( spoon.getRepository() ).thenReturn( repository );
    when( repository.getMetaStore() ).thenReturn( metaStore );

    assertThat( delegate.getRepository(), is( repository ) );
    assertThat( delegate.getMetaStore(), is( metaStore ) );
  }

}
