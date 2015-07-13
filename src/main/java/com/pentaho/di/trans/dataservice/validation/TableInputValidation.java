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
import com.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGeneration;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.CheckStepsExtension;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;

import static com.pentaho.di.trans.dataservice.validation.ValidationUtil.*;

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
