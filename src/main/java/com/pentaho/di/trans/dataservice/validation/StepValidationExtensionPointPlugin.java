/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2015 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package com.pentaho.di.trans.dataservice.validation;

import com.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.CheckStepsExtension;

import java.util.ArrayList;
import java.util.List;

import static com.pentaho.di.trans.dataservice.validation.ValidationUtil.getDataServiceMeta;


@ExtensionPoint(
    id = "StepValidationExtensionPointPlugin",
    extensionPointId = "AfterCheckStep",
    description = "Perform DataService specific step validation."
    )
public class StepValidationExtensionPointPlugin implements ExtensionPointInterface {

  private List<StepValidation> stepValidations =
      new ArrayList<StepValidation>();

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object o ) throws KettleException {
    if ( !validInputs( o, log ) ) {
      return;
    }
    CheckStepsExtension checkStepExtension = (CheckStepsExtension) o;
    DataServiceMeta dataServiceMeta = getDataServiceMeta(
        checkStepExtension.getTransMeta(), checkStepExtension.getMetaStore(), log );

    if ( dataServiceMeta == null ) {
      // We won't validate Trans not associated with a DataService
      return;
    }
    for ( StepValidation stepValidation : getStepValidations() ) {
      if ( stepValidation.supportsStep( checkStepExtension.getStepMetas()[0], log ) ) {
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
