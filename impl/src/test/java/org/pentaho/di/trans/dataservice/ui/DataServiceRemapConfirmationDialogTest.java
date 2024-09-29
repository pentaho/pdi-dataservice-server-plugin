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


package org.pentaho.di.trans.dataservice.ui;

import org.eclipse.swt.widgets.Shell;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.dataservice.ui.controller.DataServiceRemapConfirmationDialogController;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.XulLoader;
import org.pentaho.ui.xul.XulRunner;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.swt.tags.SwtDialog;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.pentaho.di.trans.dataservice.ui.DataServiceRemapConfirmationDialog.Action.REMAP;

public class DataServiceRemapConfirmationDialogTest {
  @Test
  public void testInitXul() throws KettleException, XulException {
    Shell shell = mock( Shell.class );
    XulLoader xulLoader = mock( XulLoader.class );
    XulRunner xulRunner = mock( XulRunner.class );
    XulDomContainer xulDomContainer = mock( XulDomContainer.class );
    when( xulLoader.loadXul( anyString(), any() ) ).thenReturn( xulDomContainer );

    DataServiceRemapConfirmationDialog dialog = mock( DataServiceRemapConfirmationDialog.class );
    when( dialog.initXul( shell, xulLoader, xulRunner ) ).thenCallRealMethod();
    dialog.initXul( shell, xulLoader, xulRunner );

    verify( xulLoader ).setOuterContext( shell );
    verify( xulLoader ).registerClassLoader( any( ClassLoader.class ) );
    verify( xulRunner ).addContainer( xulDomContainer );
    verify( xulRunner ).initialize();
  }

  @Test
  public void testOpen() throws XulException, KettleException {
    Shell shell = mock( Shell.class );
    final Document document = mock( Document.class );
    SwtDialog swtDialog = mock( SwtDialog.class );
    when( document.getElementById( DataServiceRemapConfirmationDialog.XUL_DIALOG_ID ) ).thenReturn( swtDialog );

    DataServiceRemapConfirmationDialogController
        controller =
        mock( DataServiceRemapConfirmationDialogController.class );
    DataServiceRemapConfirmationDialog dialog = spy( new DataServiceRemapConfirmationDialog( shell, controller ) );

    doAnswer( new Answer() {
      private int invocations = 0;

      @Override public Object answer( InvocationOnMock invocationOnMock ) throws Throwable {
        if ( invocations == 0 ) {
          invocations++;
          return document;
        } else {
          throw new XulException( "" );
        }
      }
    } ).when( dialog ).initXul( same( shell ), any( XulLoader.class ), any( XulRunner.class ) );
    dialog.open();
    verify( dialog ).initXul( same( shell ), any( XulLoader.class ), any( XulRunner.class ) );
    verify( swtDialog ).show();

    try {
      dialog.open();
    } catch ( Exception e ) {
      Assert.assertTrue( e instanceof KettleException );
    }
  }

  @Test

  public void testGetAction() {
    DataServiceRemapConfirmationDialogController
        controller =
        mock( DataServiceRemapConfirmationDialogController.class );
    when( controller.getAction() ).thenReturn( REMAP );
    DataServiceRemapConfirmationDialog
        dialog =
        new DataServiceRemapConfirmationDialog( mock( Shell.class ), controller );
    Assert.assertEquals( REMAP, dialog.getAction() );
    verify( controller ).getAction();
  }
}
