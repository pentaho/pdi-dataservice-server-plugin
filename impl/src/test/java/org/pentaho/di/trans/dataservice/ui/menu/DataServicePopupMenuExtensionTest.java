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

package org.pentaho.di.trans.dataservice.ui.menu;

import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Tree;
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
import org.pentaho.di.trans.dataservice.ui.UIFactory;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.TreeSelection;

import java.util.ArrayList;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class DataServicePopupMenuExtensionTest {

  @Mock
  private LogChannelInterface log;

  @Mock
  private Tree extension;

  @Mock
  private DataServiceContext context;

  @Mock
  private DataServiceDelegate delegate;

  @Mock
  private Spoon spoon;

  @Mock
  private TreeSelection selection;

  @Mock
  private UIFactory uiFactory;

  @Mock
  private TransMeta transMeta;

  @Mock
  private Menu menu;

  @Mock
  private MenuItem menuItem;

  @Mock
  private DataServiceMeta dataServiceMeta;

  private DataServicePopupMenuExtension dataServicePopupMenuExtension;

  @Before
  public void setUp() throws Exception {
    when( context.getDataServiceDelegate() ).thenReturn( delegate );
    when( delegate.getSpoon() ).thenReturn( spoon );
    when( spoon.getActiveTransformation() ).thenReturn( transMeta );
    when( transMeta.getSteps() ).thenReturn( new ArrayList<StepMeta>() );
    when( context.getUIFactory() ).thenReturn( uiFactory );
    when( uiFactory.getMenuItem( any(), anyInt() ) ).thenReturn( menuItem );
    when( uiFactory.getMenu( any( Tree.class ) ) ).thenReturn( menu );
    TreeSelection[] treeSelection = new TreeSelection[] { selection };
    when( spoon.getTreeObjects( any( Tree.class ) ) ).thenReturn( treeSelection );

    dataServicePopupMenuExtension = new DataServicePopupMenuExtension( context );
  }

  @Test
  public void testCallExtensionPointRoot() throws Exception {
    when( selection.getSelection() ).thenReturn( DataServiceMeta.class );

    dataServicePopupMenuExtension.callExtensionPoint( log, extension );

    verify( delegate, times( 2 ) ).getSpoon();
    verify( spoon ).getTreeObjects( extension );
    verify( uiFactory, times( 2 ) ).getMenuItem( menu, SWT.NONE );
  }

  @Test
  public void testCallExtensionPoint() throws Exception {
    when( uiFactory.getMenu( any( Tree.class ) ) ).thenReturn( null );
    when( selection.getSelection() ).thenReturn( dataServiceMeta );

    dataServicePopupMenuExtension.callExtensionPoint( log, extension );

    verify( delegate, times( 2 ) ).getSpoon();
    verify( spoon ).getTreeObjects( extension );
    verify( uiFactory, times( 5 ) ).getMenuItem( null, SWT.NONE );
  }
}
