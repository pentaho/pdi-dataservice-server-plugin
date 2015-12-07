/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.trans.TransGraph;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class DataServiceStepHandlerTest {

  private static final String STEP_NAME = "Step Name";
  private static final String HANDLER_NAME = "dataServiceStepHandler";

  @Mock
  private DataServiceDelegate delegate;

  @Mock
  private DataServiceContext context;

  @Mock
  private DataServiceStepHandler stepHandler;

  @Mock
  private StepMeta stepMeta;

  @Mock
  private TransMeta transMeta;

  @Mock
  private DataServiceMeta dataServiceMeta;

  @Mock
  private TransGraph transGraph;

  @Mock
  private Spoon spoon;

  @Before
  public void setUp() throws Exception {
    when( context.getDataServiceDelegate() ).thenReturn( delegate );
    when( delegate.getSpoon() ).thenReturn( spoon );
    when( spoon.getActiveTransformation() ).thenReturn( transMeta );
    when( spoon.getActiveTransGraph() ).thenReturn( transGraph );
    when( transGraph.getCurrentStep() ).thenReturn( stepMeta );
    when( stepMeta.getName() ).thenReturn( STEP_NAME );
    when( delegate.getDataServiceByStepName( transMeta, stepMeta.getName() ) ).thenReturn( dataServiceMeta );
    stepHandler = new DataServiceStepHandler( context );
  }

  @Test
  public void testNewDataService() {
    stepHandler.newDataService();

    verify( delegate ).createNewDataService( stepMeta.getName() );
  }

  @Test
  public void testEditDataService() {
    stepHandler.editDataService();

    verify( delegate ).editDataService( dataServiceMeta );
  }

  @Test
  public void testDeleteDataService() {
    stepHandler.deleteDataService();

    verify( delegate ).removeDataService( dataServiceMeta, true );
  }

  @Test
  public void testTestDataService() {
    stepHandler.testDataService();

    verify( delegate ).showTestDataServiceDialog( dataServiceMeta );
  }

  @Test
  public void testGetName() {
    assertEquals( HANDLER_NAME, stepHandler.getName() );
  }

  @Test
  public void testShowDriverDetailsDialog() {
    stepHandler.showDriverDetailsDialog();

    verify( delegate ).showDriverDetailsDialog();
  }
}
