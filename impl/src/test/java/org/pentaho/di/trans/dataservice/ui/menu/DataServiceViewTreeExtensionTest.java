/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.ui.menu;

import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.TreeItem;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.spoon.SelectionTreeExtension;
import org.pentaho.di.ui.spoon.Spoon;
import org.eclipse.swt.graphics.Image;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doNothing;


@RunWith( MockitoJUnitRunner.class )
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
    when( dataServiceMeta.getName() ).thenReturn( DATA_SERVICE_NAME );
  }

  @Test
  public void testCallExtension() throws Exception {
    when( delegate.getDataServices( transMeta ) ).thenReturn( dataServiceMetaList );
    when( selectionTreeExtension.getAction() ).thenReturn( Spoon.EDIT_SELECTION_EXTENSION );
    when( selectionTreeExtension.getMeta() ).thenReturn( transMeta );
    when( selectionTreeExtension.getTiRootName() ).thenReturn( treeItem );
    when( guiResource.getImage( anyString(), any( ClassLoader.class ), anyInt(), anyInt() ) ).thenReturn( image );
    when( selectionTreeExtension.getSelection() ).thenReturn( dataServiceMeta );

    dataServiceViewTreeExtension.callExtensionPoint( log, selectionTreeExtension );

    verify( selectionTreeExtension ).getSelection();
    verify( delegate ).editDataService( dataServiceMeta );
  }

  @Test
  public void testCallExtensionWithRefreshTree() throws Exception {
    when( guiResource.getImageFolder() ).thenReturn( image );
    when( selectionTreeExtension.getGuiResource() ).thenReturn( guiResource );

    doNothing().when( dataServiceViewTreeExtensionSpy ).refreshTree(
        any( SelectionTreeExtension.class ) );

    when( delegate.getDataServices( transMeta ) ).thenReturn( dataServiceMetaList );
    when( selectionTreeExtension.getAction() ).thenReturn( Spoon.REFRESH_SELECTION_EXTENSION );
    when( selectionTreeExtension.getMeta() ).thenReturn( transMeta );
    dataServiceViewTreeExtensionSpy.callExtensionPoint( log, selectionTreeExtension );

    verify( selectionTreeExtension ).getMeta();
    verify( dataServiceViewTreeExtensionSpy ).refreshTree( any( SelectionTreeExtension.class ) );
  }
}
