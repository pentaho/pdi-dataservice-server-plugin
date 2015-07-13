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
import org.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGeneration;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.CheckStepsExtension;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;

import static org.pentaho.di.trans.dataservice.validation.ValidationUtil.*;

public class TableInputValidation implements StepValidation {
  private static Class<?> PKG = TableInputValidation.class;

  @Override
  public boolean supportsStep( StepMeta step, LogChannelInterface log ) {
    return step.getStepMetaInterface() instanceof TableInputMeta;
  }

  @Override
  public void checkStep(
      CheckStepsExtension checkStepExtension, DataServiceMeta dataServiceMeta, LogChannelInterface log )
  {
    checkMissingDefinedParam( checkStepExtension, dataServiceMeta );
    checkPushdownParameter( checkStepExtension );
  }

  private void checkMissingDefinedParam( CheckStepsExtension checkStepExtension, DataServiceMeta dataServiceMeta ) {
    StepMeta checkedStepMeta = checkStepExtension.getStepMetas()[ 0 ];
    TableInputMeta tableInputMeta = (TableInputMeta) checkedStepMeta.getStepMetaInterface();

    for ( ParameterGeneration paramGen
        : getParameterGenerationsForStep( dataServiceMeta, checkedStepMeta.getName() ) ) {

      if ( Const.isEmpty( tableInputMeta.getSQL() )
          || !tableInputMeta.getSQL().contains( paramGen.getParameterName() ) ) {
        checkStepExtension.getRemarks()
            .add(
                warn(
                    BaseMessages.getString( PKG, "TableInputValidation.MissingDefinedParam.Message",
                        checkedStepMeta.getName(), paramGen.getParameterName() ), checkedStepMeta ) );
      }
    }
  }

  private void checkPushdownParameter( CheckStepsExtension checkStepExtension ) {
    StepMeta checkedStepMeta = checkStepExtension.getStepMetas()[ 0 ];
    TableInputMeta tableInputMeta = (TableInputMeta) checkedStepMeta.getStepMetaInterface();
    String sql = tableInputMeta.getSQL();
    if ( Const.isEmpty( sql ) || !paramSubstitutionModifiesString( sql, checkStepExtension.getVariableSpace() ) ) {
      // No change after variable substitution, so no parameter must have been present
      checkStepExtension.getRemarks().add( warn(
          BaseMessages.getString( PKG, "TableInputValidation.NoParameters.Message", checkedStepMeta.getName()
          ), checkedStepMeta ) );
    }
  }

}
