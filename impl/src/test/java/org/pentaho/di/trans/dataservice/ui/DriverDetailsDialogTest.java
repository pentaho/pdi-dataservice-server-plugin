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
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.XulRunner;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.impl.AbstractXulLoader;
import org.pentaho.ui.xul.swt.tags.SwtDialog;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Pavel Sakun
 */
public class DriverDetailsDialogTest {
  @Test
  public void testInitXul() throws Exception {
    DriverDetailsDialog dialog = mock( DriverDetailsDialog.class );
    Shell parentShell = mock( Shell.class );
    AbstractXulLoader xulLoader = mock( AbstractXulLoader.class );
    XulRunner xulRunner = mock( XulRunner.class );
    Document document = mock( Document.class );
    XulDomContainer container = mock( XulDomContainer.class );
    doReturn( container ).when( xulLoader ).loadXul( anyString(), any() );
    doReturn( document ).when( container ).getDocumentRoot();
    doCallRealMethod().when( dialog ).initXul( parentShell, xulLoader, xulRunner );

    assertThat( document, is( sameInstance( dialog.initXul( parentShell, xulLoader, xulRunner ) ) ) );

    verify( xulLoader ).setOuterContext( parentShell );
    verify( xulLoader ).registerClassLoader( dialog.getClass().getClassLoader() );
    verify( xulLoader ).loadXul( anyString(), any() );

    verify( container ).addEventHandler( any() );
    verify( container ).getDocumentRoot(  );

    verify( xulRunner ).addContainer( container );
    verify( xulRunner ).initialize();
  }

  @Test( expected = KettleException.class )
  public void testInitXulThrowsKettleExceptionOnError() throws KettleException, XulException {
    DriverDetailsDialog dialog = mock( DriverDetailsDialog.class );
    AbstractXulLoader xulLoader = mock( AbstractXulLoader.class );
    doCallRealMethod().when( dialog ).initXul( null, xulLoader, null );
    doThrow( new XulException() ).when( xulLoader ).loadXul( any(), any() );
    dialog.initXul( null, xulLoader, null );
  }

  @Test
  public void testOpen() throws Exception {
    DriverDetailsDialog dialog = mock( DriverDetailsDialog.class );
    doCallRealMethod().when( dialog ).open();
    SwtDialog swtDialog = mock( SwtDialog.class );
    doReturn( swtDialog ).when( dialog ).getDialog();
    dialog.open();
    verify( dialog ).getDialog();
    verify( swtDialog ).show();
  }

  @Test
  public void testClose() throws Exception {
    DriverDetailsDialog dialog = mock( DriverDetailsDialog.class );
    doCallRealMethod().when( dialog ).close();
    SwtDialog swtDialog = mock( SwtDialog.class );
    doReturn( swtDialog ).when( dialog ).getDialog();
    dialog.close();
    verify( dialog ).getDialog();
    verify( swtDialog ).dispose();
  }
}
