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

package org.pentaho.di.trans.dataservice.ui;

import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.step.StepMeta;

import java.util.Arrays;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
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
    verify( context, times( 1 ) ).removeServiceTransExecutor( dataService.getName() );
  }
}
