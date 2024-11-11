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


package org.pentaho.di.trans.dataservice.ui.controller;

import org.eclipse.swt.widgets.Shell;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.dataservice.ui.DataServiceRemapConfirmationDialog;
import org.pentaho.ui.xul.swt.tags.SwtDialog;

import java.util.ArrayList;
import java.util.Arrays;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class DataServiceRemapConfirmationDialogControllerTest {
  @Test
  public void testRemapErrorOnNoSteps() {
    DataServiceDelegate delegate = mock( DataServiceDelegate.class );

    DataServiceRemapConfirmationDialogController
        controller =
        spy( new DataServiceRemapConfirmationDialogController( mock( DataServiceMeta.class ),
            new ArrayList<String>(), delegate ) );
    doReturn( mock( SwtDialog.class ) ).when( controller ).getDialog();
    Assert.assertEquals( controller.getAction(), DataServiceRemapConfirmationDialog.Action.CANCEL );
    controller.remap();

    verify( delegate, times( 1 ) ).showRemapNoStepsDialog( Mockito.<Shell>any() );
  }

  @Test
  public void testController() {
    DataServiceDelegate delegate = mock( DataServiceDelegate.class );

    DataServiceRemapConfirmationDialogController
        controller =
        spy( new DataServiceRemapConfirmationDialogController( mock( DataServiceMeta.class ),
            Arrays.asList( "Step1" ), delegate ) );
    doReturn( mock( SwtDialog.class ) ).when( controller ).getDialog();
    Assert.assertEquals( controller.getAction(), DataServiceRemapConfirmationDialog.Action.CANCEL );
    controller.remap();
    Assert.assertEquals( controller.getAction(), DataServiceRemapConfirmationDialog.Action.REMAP );
    controller.delete();
    Assert.assertEquals( controller.getAction(), DataServiceRemapConfirmationDialog.Action.DELETE );
  }

  @Test
  public void testCancel() {
    DataServiceDelegate delegate = mock( DataServiceDelegate.class );
    DataServiceRemapConfirmationDialogController
        controller =
        spy( new DataServiceRemapConfirmationDialogController( mock( DataServiceMeta.class ),
            Arrays.asList( "Step1" ), delegate ) );
    SwtDialog dialog = mock( SwtDialog.class );
    doReturn( dialog ).when( controller ).getElementById( anyString() );
    controller.cancel();
    verify( dialog ).dispose();
  }
}
