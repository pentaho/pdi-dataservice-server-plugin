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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.ui.spoon.TreeSelection;
import org.pentaho.di.ui.spoon.delegates.SpoonTreeDelegateExtension;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bmorrise on 10/28/15.
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class DataServiceTreeDelegateExtensionTest {

  private static int CASE_NUMBER_TWO = 2;
  private static int CASE_NUMBER_THREE = 3;

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
    String[] path = new String[] { "/", DataServiceTreeDelegateExtension.STRING_DATA_SERVICES, "" };

    when( spoonTreeDelegateExtension.getCaseNumber() ).thenReturn( CASE_NUMBER_TWO );
    when( spoonTreeDelegateExtension.getTransMeta() ).thenReturn( transMeta );
    when( spoonTreeDelegateExtension.getPath() ).thenReturn( path );
    when( spoonTreeDelegateExtension.getObjects() ).thenReturn( objects );

    dataServiceTreeDelegateExtension.callExtensionPoint( log, spoonTreeDelegateExtension );

    when( spoonTreeDelegateExtension.getCaseNumber() ).thenReturn( CASE_NUMBER_THREE );
    when( metaStoreUtil.getDataService( "", transMeta ) ).thenReturn( dataServiceMeta );

    dataServiceTreeDelegateExtension.callExtensionPoint( log, spoonTreeDelegateExtension );

    verify( spoonTreeDelegateExtension, times( 2 ) ).getCaseNumber();
    verify( spoonTreeDelegateExtension, times( 2 ) ).getPath();
    assertThat( objects.isEmpty(), is( false ) );
  }

}
