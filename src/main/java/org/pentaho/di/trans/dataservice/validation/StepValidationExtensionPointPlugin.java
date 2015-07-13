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

package org.pentaho.di.trans.dataservice.validation;

import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.DataServiceMetaStoreUtil;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.CheckStepsExtension;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

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

  public StepValidationExtensionPointPlugin( DataServiceMetaStoreUtil metaStoreUtil ) {
    this.metaStoreUtil = metaStoreUtil;
  }

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object o ) throws KettleException {
    if ( !validInputs( o, log ) ) {
      return;
    }
    CheckStepsExtension checkStepExtension = (CheckStepsExtension) o;
    for ( StepValidation stepValidation : getStepValidations() ) {
      StepMeta stepMeta = checkStepExtension.getStepMetas()[0];
      if ( stepValidation.supportsStep( stepMeta , log ) ) {
        DataServiceMeta dataServiceMeta = getDataServiceMeta(
          checkStepExtension.getTransMeta(), checkStepExtension.getMetaStore(), stepMeta.getName(), log );

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

  private DataServiceMeta getDataServiceMeta(
    TransMeta transMeta, IMetaStore metaStore, String stepName, LogChannelInterface log ) {
    if ( metaStore == null || transMeta == null ) {
      log.logBasic(
        String.format( "Unable to determine whether '%s' is associated with a DataService.",
          transMeta == null ? "(unknown)" : transMeta.getName() ) );
      return null;
    }
    try {
      return metaStoreUtil.fromTransMeta( transMeta, metaStore, stepName );
    } catch ( MetaStoreException e ) {
      log.logError(
        String.format(
          "Error while attempting to load DataServiceMeta during step validation for '%s'.",
          transMeta.getName() ),
        e );
    }
    return null;
  }
}
