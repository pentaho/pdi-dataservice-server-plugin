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

package org.pentaho.di.trans.dataservice.ui;

import java.util.Arrays;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.step.StepMeta;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataServiceStepDeleteExtensionPointPluginTest {
  @Test
  public void testCallExtensionPoint() throws Exception {
    DataServiceContext context = mock( DataServiceContext.class );
    TransMeta trans = mock( TransMeta.class );

    DataServiceDelegate delegate = mock( DataServiceDelegate.class );
    when( context.getDataServiceDelegate() ).thenReturn( delegate );
    when( delegate.showRemapConfirmationDialog( any( DataServiceMeta.class ), anyList() ) ).thenReturn(
        DataServiceRemapConfirmationDialog.Action.CANCEL, DataServiceRemapConfirmationDialog.Action.REMAP,
        DataServiceRemapConfirmationDialog.Action.DELETE );
    when( delegate.showRemapStepChooserDialog( any( DataServiceMeta.class ), anyList(), any( TransMeta.class ) ) )
        .thenReturn(
            DataServiceRemapStepChooserDialog.Action.CANCEL );

    StepMeta step1 = mock( StepMeta.class );
    when( step1.getParentTransMeta() ).thenReturn( trans );
    when( step1.getName() ).thenReturn( "Step1" );

    when( trans.getStepNames() ).thenReturn( new String[] { "Step1" } );

    DataServiceMeta dataService = mock( DataServiceMeta.class );
    when( dataService.getStepname() ).thenReturn( "Step1" );
    when( delegate.getDataServices( trans ) ).thenReturn( Arrays.asList( dataService ) );
    when( delegate.getDataServiceByStepName( trans, "Step1" ) ).thenReturn( dataService );

    StepMeta[] steps = new StepMeta[] { step1 };

    DataServiceStepDeleteExtensionPointPlugin plugin = new DataServiceStepDeleteExtensionPointPlugin( context );
    try {
      plugin.callExtensionPoint( null, steps );
    } catch ( KettleException ke ) {
      // KettleException will be thrown on cancel
    }

    verify( delegate, times( 0 ) )
        .showRemapStepChooserDialog( any( DataServiceMeta.class ), anyList(), any( TransMeta.class ) );

    try {
      plugin.callExtensionPoint( null, steps );
    } catch ( KettleException ke ) {
      // KettleException will be thrown on cancel
    }

    verify( delegate, times( 1 ) )
        .showRemapStepChooserDialog( any( DataServiceMeta.class ), anyList(), any( TransMeta.class ) );

    plugin.callExtensionPoint( null, steps );

    verify( delegate, times( 1 ) ).removeDataService( dataService );
  }
}
