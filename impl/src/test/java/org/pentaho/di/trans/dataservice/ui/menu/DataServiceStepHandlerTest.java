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
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.trans.TransGraph;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.StrictStubs.class)
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
  public void testNewDataServiceNoTransientDS() {
    when( dataServiceMeta .isUserDefined() ).thenReturn( true );
    stepHandler.newDataService();
    verify( delegate, never() ).removeDataService( dataServiceMeta );
    verify( delegate ).createNewDataService( stepMeta.getName() );
  }

  @Test
  public void testNewDataServiceTransientDS() {
    when( dataServiceMeta .isUserDefined() ).thenReturn( false );
    stepHandler.newDataService();
    verify( delegate ).removeDataService( dataServiceMeta );
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
