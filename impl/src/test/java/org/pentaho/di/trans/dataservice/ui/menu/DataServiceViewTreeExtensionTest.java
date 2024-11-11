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


package org.pentaho.di.trans.dataservice.ui.menu;

import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.TreeItem;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.spoon.SelectionTreeExtension;
import org.pentaho.di.ui.spoon.Spoon;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class DataServiceViewTreeExtensionTest {

  private static String DATA_SERVICE_NAME = "Data Service Name";

  @Mock
  DataServiceContext context;

  @Mock
  DataServiceDelegate delegate;

  @Mock
  LogChannelInterface log;

  @Mock
  SelectionTreeExtension selectionTreeExtension;

  @Mock
  TransMeta transMeta;

  @Mock
  TreeItem treeItem;

  @Mock
  GUIResource guiResource;

  Display display;

  Image image;

  @Mock
  DataServiceMeta dataServiceMeta;

  DataServiceViewTreeExtension dataServiceViewTreeExtension;
  DataServiceViewTreeExtension dataServiceViewTreeExtensionSpy;
  List<DataServiceMeta> dataServiceMetaList = new ArrayList<>();

  @Before
  public void setUp() throws Exception {
    when( context.getDataServiceDelegate() ).thenReturn( delegate );

    dataServiceViewTreeExtension = new DataServiceViewTreeExtension( context );
    dataServiceViewTreeExtensionSpy = spy( new DataServiceViewTreeExtension( context ) );

    dataServiceMetaList.add( dataServiceMeta );
  }

  @Test
  public void testCallExtension() throws Exception {
    when( selectionTreeExtension.getAction() ).thenReturn( Spoon.EDIT_SELECTION_EXTENSION );
    when( selectionTreeExtension.getSelection() ).thenReturn( dataServiceMeta );

    dataServiceViewTreeExtension.callExtensionPoint( log, selectionTreeExtension );

    verify( selectionTreeExtension ).getSelection();
    verify( delegate ).editDataService( dataServiceMeta );
  }

  @Test
  public void testCallExtensionWithRefreshTree() throws Exception {
    doNothing().when( dataServiceViewTreeExtensionSpy ).refreshTree(
        any( SelectionTreeExtension.class ) );

    when( selectionTreeExtension.getAction() ).thenReturn( Spoon.REFRESH_SELECTION_EXTENSION );
    when( selectionTreeExtension.getMeta() ).thenReturn( transMeta );
    dataServiceViewTreeExtensionSpy.callExtensionPoint( log, selectionTreeExtension );

    verify( selectionTreeExtension ).getMeta();
    verify( dataServiceViewTreeExtensionSpy ).refreshTree( any( SelectionTreeExtension.class ) );
  }
}
