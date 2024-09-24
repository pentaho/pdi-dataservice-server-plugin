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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.collections.ListUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.step.StepMeta;

@ExtensionPoint(
    id = "DataServiceStepDeleteExtensionPointPlugin",
    extensionPointId = "TransBeforeDeleteSteps",
    description = "Handles deletion of steps to prevent leaving orphaned data services"
  )

public class DataServiceStepDeleteExtensionPointPlugin implements ExtensionPointInterface {
  private DataServiceContext context;
  private DataServiceDelegate dataServiceDelegate;

  public DataServiceStepDeleteExtensionPointPlugin( DataServiceContext context ) {
    this.context = context;
    this.dataServiceDelegate = context.getDataServiceDelegate();
  }

  @Override public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    if ( object != null && object instanceof StepMeta[] ) {
      StepMeta[] stepsToBeRemoved = (StepMeta[]) object;
      TransMeta trans = stepsToBeRemoved[0].getParentTransMeta();
      List<String> stepsToBeRemovedNames = new ArrayList<>( stepsToBeRemoved.length );
      for ( StepMeta step : stepsToBeRemoved ) {
        stepsToBeRemovedNames.add( step.getName() );
      }
      List<String> transSteps = Arrays.asList( trans.getStepNames() );
      List<String> remainingStepNames = ListUtils.subtract( transSteps, stepsToBeRemovedNames );

      for ( StepMeta stepToBeRemoved : stepsToBeRemoved ) {
        DataServiceMeta dataService = dataServiceDelegate
            .getDataServiceByStepName( stepToBeRemoved.getParentTransMeta(), stepToBeRemoved.getName() );
        if ( dataService != null ) {
          List<String> dataServiceSteps = new ArrayList<>();
          for ( DataServiceMeta ds : dataServiceDelegate.getDataServices( stepToBeRemoved.getParentTransMeta() ) ) {
            dataServiceSteps.add( ds.getStepname() );
          }

          switch ( dataServiceDelegate.showRemapConfirmationDialog( dataService,
              ListUtils.subtract( remainingStepNames, dataServiceSteps ) ) ) {
            case CANCEL:
              throw new KettleException( "Steps deletion cancelled by user" );
            case REMAP:
              if ( dataServiceDelegate
                  .showRemapStepChooserDialog( dataService, ListUtils.subtract( remainingStepNames, dataServiceSteps ),
                      trans )
                  == DataServiceRemapStepChooserDialog.Action.CANCEL ) {
                throw new KettleException( "Steps deletion cancelled by user" );
              }

              break;
            case DELETE:
              context.removeServiceTransExecutor( dataService.getName() );
              context.getDataServiceDelegate().removeDataService( dataService );
              break;
          }
        }
      }
    }
  }
}
