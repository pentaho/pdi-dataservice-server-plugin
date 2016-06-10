/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.spoon.trans.StepMenuExtension;
import org.pentaho.di.ui.spoon.trans.TransGraph;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.containers.XulMenupopup;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

@RunWith( MockitoJUnitRunner.class )
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
