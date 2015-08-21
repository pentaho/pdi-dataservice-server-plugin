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

package org.pentaho.di.trans.dataservice.ui.controller;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith( MockitoJUnitRunner.class )
public class DataServiceDialogControllerTest {

  private DataServiceDialogController controller;

  @Mock DataServiceModel model;

  @Mock DataServiceMeta dataService;

  @Mock DataServiceDelegate delegate;

  private static final String FILE_NAME = "/home/admin/transformation.ktr";

  private static final String SERVICE_NAME = "test_service";

  private static final String NEW_SERVICE_NAME = "test_service2";

  private static final String SELECTED_STEP = "Output Step";

  private static final String STEP_ONE_NAME = "Step One";

  private static final String STEP_TWO_NAME = "Step Two";

  @Before
  public void init() throws Exception {
    controller = new DataServiceDialogController( model, delegate );

    doReturn( SERVICE_NAME ).when( dataService ).getName();
  }

  @Test
  public void testValidate() throws Exception {
    controller.setDataService( dataService );
    doReturn( SERVICE_NAME ).when( model ).getServiceName();
    doReturn( SELECTED_STEP ).when( model ).getServiceStep();
    doReturn( true ).when( delegate ).saveAllowed( SERVICE_NAME, dataService );

    assertThat( controller.validate(), is( true ) );

    verify( delegate ).saveAllowed( SERVICE_NAME, dataService );
  }

  @Test
  public void testError() throws Exception {
    doReturn( "" ).when( model ).getServiceName();

    assertFalse( controller.validate() );

    doReturn( SERVICE_NAME ).when( model ).getServiceName();
    doReturn( "" ).when( model ).getServiceStep();

    assertFalse( controller.validate() );

    doReturn( SELECTED_STEP ).when( model ).getServiceStep();
    controller.setDataService( dataService );
    doReturn( false ).when( delegate ).saveAllowed( SERVICE_NAME, dataService );

    assertFalse( controller.validate() );

    doReturn( true ).when( delegate ).saveAllowed( SERVICE_NAME, dataService );
    assertTrue( controller.validate() );

    verify( delegate, times( 3 ) ).showError( anyString(), anyString() );
  }
}
