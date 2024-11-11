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

import java.util.List;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Tree;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.controller.DataServiceRemapConfirmationDialogController;
import org.pentaho.di.trans.dataservice.ui.controller.DataServiceRemapNoStepsDialogController;
import org.pentaho.di.trans.dataservice.ui.controller.DataServiceRemapStepChooserDialogController;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceRemapStepChooserModel;
import org.pentaho.ui.xul.XulException;

public class UIFactory {
  public Menu getMenu( Tree tree ) {
    return new Menu( tree );
  }

  public MenuItem getMenuItem( Menu parent, int style ) {
    return new MenuItem( parent, style );
  }

  public MessageBox getMessageBox( Shell shell, int flags ) {
    return new MessageBox( shell, flags );
  }

  public MessageDialog getMessageDialog( Shell parentShell, String dialogTitle, Image dialogTitleImage,
      String dialogMessage, int dialogImageType, String[] dialogButtonLabels, int defaultIndex ) {
    return new MessageDialog( parentShell, dialogTitle, dialogTitleImage, dialogMessage, dialogImageType,
        dialogButtonLabels, defaultIndex );
  }

  public DataServiceDialog.Builder getDataServiceDialogBuilder( TransMeta transMeta ) {
    return new DataServiceDialog.Builder( transMeta );
  }

  public DataServiceTestDialog getDataServiceTestDialog( Shell shell, DataServiceMeta dataServiceMeta,
      DataServiceContext context ) throws KettleException {
    return new DataServiceTestDialog( shell, dataServiceMeta, context );
  }

  public DriverDetailsDialog getDriverDetailsDialog( Shell shell ) throws
      KettleException, XulException {
    return new DriverDetailsDialog( shell );
  }

  public DataServiceRemapConfirmationDialog getRemapConfirmationDialog( Shell shell, DataServiceMeta dataService,
      List<String> remainingStepsNames, DataServiceDelegate dataServiceDelegate ) throws KettleException {
    DataServiceRemapConfirmationDialogController
        controller =
        new DataServiceRemapConfirmationDialogController( dataService, remainingStepsNames, dataServiceDelegate );
    return new DataServiceRemapConfirmationDialog( shell, controller );
  }

  public DataServiceRemapStepChooserDialog getRemapStepChooserDialog( Shell shell, DataServiceMeta dataService,
      List<String> remainingStepsNames, DataServiceDelegate dataServiceDelegate )
      throws KettleException {
    DataServiceRemapStepChooserModel model = new DataServiceRemapStepChooserModel();
    DataServiceRemapStepChooserDialogController
        controller =
        new DataServiceRemapStepChooserDialogController( model, dataService, remainingStepsNames, dataServiceDelegate );
    return new DataServiceRemapStepChooserDialog( shell, controller );
  }

  public DataServiceRemapNoStepsDialog getRemapNoStepsDialog( Shell shell ) throws KettleException {
    DataServiceRemapNoStepsDialogController controller = new DataServiceRemapNoStepsDialogController();
    return new DataServiceRemapNoStepsDialog( shell, controller );
  }

}
