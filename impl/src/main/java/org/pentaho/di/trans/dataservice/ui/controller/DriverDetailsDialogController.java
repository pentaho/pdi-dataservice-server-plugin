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

import org.apache.commons.io.FileUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.FileDialog;
import org.pentaho.di.core.Const;
import org.pentaho.di.trans.dataservice.ui.DriverDetailsDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.util.HelpUtils;
import org.pentaho.platform.engine.core.system.PentahoSystem;
import org.pentaho.ui.xul.swt.tags.SwtDialog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NotDirectoryException;

import static org.pentaho.di.i18n.BaseMessages.getString;

public class DriverDetailsDialogController extends AbstractController {
  private static final String NAME = "driverDetailsDialogController";
  private static final Class<?> PKG = DriverDetailsDialog.class;

  public DriverDetailsDialogController() {
    setName( NAME );
  }

  @SuppressWarnings( "unused" ) // Bound via XUL
  public void saveDriver() {
    try {
      File driverBundle = getDriverBundle();
      File saveLocation = getDriverSaveLocation();

      if ( saveLocation == null ) {
        return;
      }

      FileUtils.copyFile( driverBundle, saveLocation );
    } catch ( IOException ioe ) {
      showErrorDialog( ioe );
      ioe.printStackTrace();
    }
  }

  ErrorDialog showErrorDialog( IOException ioe ) {
    return new ErrorDialog( getDialog().getShell(), getString( PKG, "DriverDetailsDialog.ErrorCopyingDriver.Title" ),
        getString( PKG, "DriverDetailsDialog.ErrorCopyingDriver.Message" ), ioe );
  }

  @SuppressWarnings( "unused" ) // Bound via XUL
  public void showHelp() {
    String label = getString( PKG, "DriverDetailsDialog.ConnectionSetupLink.Label" );
    String docUrl = Const.getDocUrl( getString( PKG, "DriverDetailsDialog.ConnectionSetupLink.Url" ) );
    String header = "";

    HelpUtils.openHelpDialog( getDialog().getShell(), label, docUrl, header );
  }

  @SuppressWarnings( "unused" ) // Bound via XUL
  public void close() {
    getDialog().dispose();
  }

  SwtDialog getDialog() {
    return getElementById( DriverDetailsDialog.XUL_DIALOG_ID );
  }

  File getDriverBundle() throws FileNotFoundException, AccessDeniedException {
    String driverFolderPath =
        getString( PKG, "DriverDetailsDialog.DriverZipPath", PentahoSystem.getApplicationContext()
            .getSolutionRootPath() );
    File[] matchingFiles = new File( driverFolderPath ).listFiles( new FilenameFilter() {
      public boolean accept( File dir, String name ) {
        return name.startsWith( getString( PKG, "DriverDetailsDialog.DriverZipNamePrefix" ) ) && name.endsWith( "zip" );
      }
    } );

    if ( matchingFiles == null || matchingFiles.length == 0 || matchingFiles[0] == null || !matchingFiles[0]
        .exists() ) {
      throw new FileNotFoundException( getString( PKG, "DriverDetailsDialog.DriverBundleNotFoundError.Message",
          driverFolderPath ) );
    }

    return matchingFiles[0];
  }

  File getDriverSaveLocation() throws NotDirectoryException, AccessDeniedException, FileNotFoundException {
    FileDialog dialog = new FileDialog( getDialog().getShell(), SWT.SAVE );
    dialog.setFilterNames( new String[] { getString( PKG, "DriverDetailsDialog.SaveDialog.ZipFilesFilter.Label" ),
        getString( PKG, "DriverDetailsDialog.SaveDialog.AllFilesFilter.Label" ) } );
    dialog.setFilterExtensions( new String[] { "*.zip", "*.*" } );
    dialog.setFileName( getString( PKG, "DriverDetailsDialog.SaveDialog.DefaultFileName.Label" ) );
    String savePath = dialog.open();

    if ( savePath != null ) {
      return new File( savePath );
    }

    return null;
  }
}
