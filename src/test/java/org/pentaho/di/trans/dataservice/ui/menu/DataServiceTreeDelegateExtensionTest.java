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

package org.pentaho.di.trans.dataservice.ui.menu;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.trans.dataservice.ui.menu.DataServiceTreeDelegateExtension;
import org.pentaho.di.ui.spoon.TreeSelection;
import org.pentaho.di.ui.spoon.delegates.SpoonTreeDelegateExtension;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

/**
 * Created by bmorrise on 10/28/15.
 */
@RunWith( MockitoJUnitRunner.class )
public class DataServiceTreeDelegateExtensionTest {

  private static int CASE_NUMBER_THREE = 3;
  private static int CASE_NUMBER_FOUR = 4;

  @Mock DataServiceMetaStoreUtil metaStoreUtil;

  @Mock DataServiceContext context;

  @Mock LogChannelInterface log;

  @Mock SpoonTreeDelegateExtension spoonTreeDelegateExtension;

  @Mock TransMeta transMeta;

  @Mock DataServiceMeta dataServiceMeta;

  DataServiceTreeDelegateExtension dataServiceTreeDelegateExtension;

  @Before public void setUp() throws Exception {
    when( context.getMetaStoreUtil() ).thenReturn( metaStoreUtil );

    dataServiceTreeDelegateExtension = new DataServiceTreeDelegateExtension( context );
  }

  @Test public void testCallExtensionPoint() throws Exception {

    List<TreeSelection> objects = new ArrayList<>();
    String[] path = new String[] { "/", "/", DataServiceTreeDelegateExtension.STRING_DATA_SERVICES, "" };

    when( spoonTreeDelegateExtension.getCaseNumber() ).thenReturn( CASE_NUMBER_THREE );
    when( spoonTreeDelegateExtension.getTransMeta() ).thenReturn( transMeta );
    when( spoonTreeDelegateExtension.getPath() ).thenReturn( path );
    when( spoonTreeDelegateExtension.getObjects() ).thenReturn( objects );

    dataServiceTreeDelegateExtension.callExtensionPoint( log, spoonTreeDelegateExtension );

    when( spoonTreeDelegateExtension.getCaseNumber() ).thenReturn( CASE_NUMBER_FOUR );
    when( metaStoreUtil.getDataService( "", transMeta ) ).thenReturn( dataServiceMeta );

    dataServiceTreeDelegateExtension.callExtensionPoint( log, spoonTreeDelegateExtension );

    verify( spoonTreeDelegateExtension, times( 2 ) ).getCaseNumber();
    verify( spoonTreeDelegateExtension, times( 2 ) ).getPath();
    assertThat( objects.isEmpty(), is( false ) );
  }

}
