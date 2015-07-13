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

package org.pentaho.di.trans.dataservice.validation;

import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGeneration;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.CheckStepsExtension;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;

import java.util.List;

import static org.pentaho.di.trans.dataservice.validation.ValidationUtil.*;

public class MongoDbBasicValidation implements StepValidation {
  private static Class<?> PKG = MongoDbBasicValidation.class;

  @Override
  public boolean supportsStep( StepMeta step, LogChannelInterface log ) {
    return step.getStepMetaInterface() instanceof MongoDbInputMeta;
  }

  @Override
  public void checkStep(
      CheckStepsExtension checkStepExtension, DataServiceMeta dataServiceMeta, LogChannelInterface log )
  {
    StepMeta stepMeta = checkStepExtension.getStepMetas()[0];
    List<CheckResultInterface> remarks = checkStepExtension.getRemarks();
    MongoDbInputMeta mongoDbMeta = (MongoDbInputMeta) checkStepExtension.getStepMetas()[0].getStepMetaInterface();
    VariableSpace space = checkStepExtension.getVariableSpace();

    checkOutputJson( stepMeta, remarks, mongoDbMeta );
    checkPushdownParameter( stepMeta, remarks, mongoDbMeta, space );
    checkParameterGenerationOptimizedMeta( checkStepExtension, dataServiceMeta, log );
  }

  private void checkParameterGenerationOptimizedMeta(
      CheckStepsExtension checkStepExtension, DataServiceMeta dataServiceMeta, LogChannelInterface log )
  {
    StepMeta checkedStepMeta = checkStepExtension.getStepMetas()[0];

    for ( ParameterGeneration paramGen : getParameterGenerationsForStep( dataServiceMeta, checkedStepMeta.getName() ) )
    {
      MongoDbInputMeta mongoDbMeta = (MongoDbInputMeta) checkStepExtension.getStepMetas()[0].getStepMetaInterface();
      if ( Const.isEmpty( mongoDbMeta.getJsonQuery() )
          || !mongoDbMeta.getJsonQuery().contains( paramGen.getParameterName() ) ) {
        checkStepExtension
            .getRemarks()
            .add( warn(
            BaseMessages.getString( PKG, "MongoDbBasicValidation.MissingDefinedParam.Message",
              checkedStepMeta.getName(), paramGen.getParameterName() ), checkedStepMeta ) );
      }
    }
  }

  private void checkPushdownParameter(
      StepMeta stepMeta, List<CheckResultInterface> remarks, MongoDbInputMeta mongoDbMeta, VariableSpace space )
  {
    String json = mongoDbMeta.getJsonQuery();
    if ( Const.isEmpty( json ) || !paramSubstitutionModifiesString( json, space ) ) {
      // No change after variable substitution, so no parameter must have been present
      remarks.add( warn(
          BaseMessages.getString( PKG, "MongoDbBasicValidation.NoParameters.Message", stepMeta.getName()
        ), stepMeta ) );
    }
  }

  private void checkOutputJson( StepMeta stepMeta, List<CheckResultInterface> remarks, MongoDbInputMeta mongoDbMeta ) {
    if ( mongoDbMeta.getOutputJson() ) {
      remarks.add( warn(
          BaseMessages.getString( PKG, "MongoDbBasicValidation.JsonOutputType.Message", stepMeta.getName()
        ), stepMeta ) );
    }
  }
}
