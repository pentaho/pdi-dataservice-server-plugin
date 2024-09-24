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
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.spoon.trans.StepMenuExtension;
import org.pentaho.di.ui.spoon.trans.TransGraph;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.containers.XulMenupopup;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class DataServiceStepMenuExtensionTest {

  private static final String STEP_NAME = "Step Name";

  @Mock
  private DataServiceMetaStoreUtil metaStoreUtil;

  @Mock
  private DataServiceContext context;

  @Mock
  private LogChannelInterface log;

  @Mock
  private StepMenuExtension stepMenuExtension;

  @Mock
  private TransGraph transGraph;

  @Mock
  private TransMeta transMeta;

  @Mock
  private StepMeta stepMeta;

  @Mock
  private XulMenupopup menupopup;

  @Mock
  private XulComponent component;

  @Mock
  private DataServiceMeta dataServiceMeta;

  private DataServiceStepMenuExtension dataServiceStepMenuExtension;

  @Before
  public void setUp() throws Exception {
    when( context.getMetaStoreUtil() ).thenReturn( metaStoreUtil );
    dataServiceStepMenuExtension = new DataServiceStepMenuExtension( context );
  }

  @Test
  public void testCallExtensionPoint() throws Exception {
    when( stepMenuExtension.getTransGraph() ).thenReturn( transGraph );
    when( transGraph.getTransMeta() ).thenReturn( transMeta );
    when( transGraph.getCurrentStep() ).thenReturn( stepMeta );
    when( stepMenuExtension.getMenu() ).thenReturn( menupopup );
    when( stepMeta.getName() ).thenReturn( STEP_NAME );
    when( dataServiceMeta.isUserDefined() ).thenReturn( true );
    when( metaStoreUtil.getDataServiceByStepName( transMeta, stepMeta.getName() ) ).thenReturn( dataServiceMeta );
    when( menupopup.getElementById( anyString() ) ).thenReturn( component );

    dataServiceStepMenuExtension.callExtensionPoint( log, stepMenuExtension );

    verify( stepMenuExtension, times( 2 ) ).getTransGraph();
    verify( transGraph ).getTransMeta();
    verify( transGraph ).getCurrentStep();
    verify( stepMenuExtension ).getMenu();
    verify( metaStoreUtil ).getDataServiceByStepName( transMeta, stepMeta.getName() );
    verify( menupopup, times( 4 ) ).getElementById( anyString() );
    verify( component, times( 3 ) ).setDisabled( false );
    verify( component, times( 1 ) ).setDisabled( true );
  }

  @Test
  public void testCallExtensionPointWithTransient() throws Exception {
    when( stepMenuExtension.getTransGraph() ).thenReturn( transGraph );
    when( transGraph.getTransMeta() ).thenReturn( transMeta );
    when( transGraph.getCurrentStep() ).thenReturn( stepMeta );
    when( stepMenuExtension.getMenu() ).thenReturn( menupopup );
    when( stepMeta.getName() ).thenReturn( STEP_NAME );
    when( dataServiceMeta.isUserDefined() ).thenReturn( false );
    when( metaStoreUtil.getDataServiceByStepName( transMeta, stepMeta.getName() ) ).thenReturn( dataServiceMeta );
    when( menupopup.getElementById( anyString() ) ).thenReturn( component );

    dataServiceStepMenuExtension.callExtensionPoint( log, stepMenuExtension );

    verify( stepMenuExtension, times( 2 ) ).getTransGraph();
    verify( transGraph ).getTransMeta();
    verify( transGraph ).getCurrentStep();
    verify( stepMenuExtension ).getMenu();
    verify( metaStoreUtil ).getDataServiceByStepName( transMeta, stepMeta.getName() );
    verify( menupopup, times( 4 ) ).getElementById( anyString() );
    verify( component, times( 1 ) ).setDisabled( false );
    verify( component, times( 3 ) ).setDisabled( true );
  }


}
