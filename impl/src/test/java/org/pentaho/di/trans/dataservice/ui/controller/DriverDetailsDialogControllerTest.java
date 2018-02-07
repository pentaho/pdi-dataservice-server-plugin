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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.trans.dataservice.ui.DriverDetailsDialog;
import org.pentaho.ui.xul.swt.tags.SwtDialog;

import java.io.FileNotFoundException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NotDirectoryException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith( MockitoJUnitRunner.class )
public class DriverDetailsDialogControllerTest {
  @Mock
  DriverDetailsDialogController controller;

  @Test
  public void testConstructor() {
    assertThat( new DriverDetailsDialogController().getName(), is( not( emptyOrNullString() ) ) );
  }

  @Test
  public void testSaveDriver() throws Exception {
    doCallRealMethod().when( controller ).saveDriver();
    doReturn( null ).when( controller ).getDriverSaveLocation();

    controller.saveDriver();

    verify( controller ).getDriverBundle();
    verify( controller ).getDriverSaveLocation();
  }

  @Test
  public void testSaveDriverReportsError() throws FileNotFoundException, AccessDeniedException, NotDirectoryException {
    AccessDeniedException ade = mock( AccessDeniedException.class );
    doCallRealMethod().when( controller ).saveDriver();
    doThrow( ade ).when( controller ).getDriverSaveLocation();

    controller.saveDriver();

    verify( controller ).showErrorDialog( ade );
    verify( ade ).printStackTrace();
  }

  @Test
  public void testShowHelpUsesDialogShell() throws Exception {
    doCallRealMethod().when( controller ).showHelp();
    SwtDialog dialog = mock( SwtDialog.class );
    doReturn( dialog ).when( controller ).getDialog();
    IllegalStateException runtimeException = new IllegalStateException();
    doThrow( runtimeException ).when( dialog ).getShell();

    try {
      controller.showHelp();
    } catch ( Exception e ) {
      assertThat( runtimeException, is( sameInstance( e ) ) );
    }

    verify( dialog ).getShell();
  }

  @Test
  public void testCloseDisposesDialog() throws Exception {
    doCallRealMethod().when( controller ).close();
    SwtDialog dialog = mock( SwtDialog.class );
    doReturn( dialog ).when( controller ).getDialog();

    controller.close();

    verify( dialog ).dispose();
  }

  @Test
  public void testGetDialog() throws Exception {
    doCallRealMethod().when( controller ).getDialog();
    SwtDialog dialog = mock( SwtDialog.class );
    doReturn( dialog ).when( controller ).getElementById( DriverDetailsDialog.XUL_DIALOG_ID );

    assertThat( dialog, is( sameInstance( controller.getDialog() ) ) );
  }
}
