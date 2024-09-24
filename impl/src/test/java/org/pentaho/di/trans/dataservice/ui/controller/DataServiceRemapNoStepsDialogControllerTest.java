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

import org.junit.Test;
import org.pentaho.ui.xul.swt.tags.SwtDialog;
import org.springframework.util.Assert;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class DataServiceRemapNoStepsDialogControllerTest {
  @Test
  public void testController() {
    SwtDialog dialog = mock( SwtDialog.class );

    DataServiceRemapNoStepsDialogController controller = spy( new DataServiceRemapNoStepsDialogController() );
    Assert.hasText( controller.getName() );
    doReturn( dialog ).when( controller ).getElementById( anyString() );
    controller.close();
    verify( dialog ).dispose();
  }
}
