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

package org.pentaho.di.trans.dataservice.validation;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.CheckStepsExtension;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.trans.step.StepMeta;

import java.util.ArrayList;
import java.util.List;

@ExtensionPoint(
    id = "StepValidationExtensionPointPlugin",
    extensionPointId = "AfterCheckStep",
    description = "Perform DataService specific step validation."
    )
public class StepValidationExtensionPointPlugin implements ExtensionPointInterface {

  private final DataServiceMetaStoreUtil metaStoreUtil;
  private List<StepValidation> stepValidations =
      new ArrayList<StepValidation>();

  public StepValidationExtensionPointPlugin( DataServiceContext context ) {
    this.metaStoreUtil = context.getMetaStoreUtil();
  }

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object o ) throws KettleException {
    if ( !validInputs( o, log ) ) {
      return;
    }
    CheckStepsExtension checkStepExtension = (CheckStepsExtension) o;
    TransMeta transMeta = checkStepExtension.getTransMeta();
    for ( StepValidation stepValidation : getStepValidations() ) {
      StepMeta stepMeta = checkStepExtension.getStepMetas()[0];
      if ( stepValidation.supportsStep( stepMeta, log ) ) {
        DataServiceMeta dataServiceMeta = metaStoreUtil.getDataServiceByStepName( transMeta, stepMeta.getName() );

        if ( dataServiceMeta == null ) {
          // We won't validate Trans not associated with a DataService
          return;
        }
        stepValidation.checkStep( checkStepExtension, dataServiceMeta, log );
      }
    }
  }

  private boolean validInputs( Object o, LogChannelInterface log ) {
    if ( !( o instanceof CheckStepsExtension ) ) {
      log.logError(
          "StepValidationExtensionPointPlugin invoked with wrong object type, should be CheckStepsExtension" );
      return false;
    }
    CheckStepsExtension checkStepExtension = (CheckStepsExtension) o;
    if ( checkStepExtension.getStepMetas().length != 1 ) {
      log.logError(
          "StepValidationExtensionPointPlugin invoked with wrong number of StepMeta objects.  "
          + "Should be 1 step." );
      return false;
    }
    return true;

  }

  public List<StepValidation> getStepValidations() {
    return stepValidations;
  }

  public void setStepValidations( List<StepValidation> stepValidations ) {
    this.stepValidations = stepValidations;
  }
}
