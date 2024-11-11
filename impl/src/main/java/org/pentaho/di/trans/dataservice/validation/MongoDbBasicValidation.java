/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


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

import static org.pentaho.di.trans.dataservice.validation.ValidationUtil.getParameterGenerationsForStep;
import static org.pentaho.di.trans.dataservice.validation.ValidationUtil.paramSubstitutionModifiesString;
import static org.pentaho.di.trans.dataservice.validation.ValidationUtil.warn;

public class MongoDbBasicValidation implements StepValidation {
  private static Class<?> PKG = MongoDbBasicValidation.class;

  @Override
  public boolean supportsStep( StepMeta step, LogChannelInterface log ) {
    try {
      return step.getStepMetaInterface() instanceof MongoDbInputMeta;
    } catch ( NoClassDefFoundError e ) {
      return false;
    }
  }

  @Override
  public void checkStep(
      CheckStepsExtension checkStepExtension, DataServiceMeta dataServiceMeta, LogChannelInterface log ) {
    StepMeta stepMeta = checkStepExtension.getStepMetas()[0];
    List<CheckResultInterface> remarks = checkStepExtension.getRemarks();
    MongoDbInputMeta mongoDbMeta = (MongoDbInputMeta) checkStepExtension.getStepMetas()[0].getStepMetaInterface();
    VariableSpace space = checkStepExtension.getVariableSpace();

    checkOutputJson( stepMeta, remarks, mongoDbMeta );
    checkPushdownParameter( stepMeta, remarks, mongoDbMeta, space );
    checkParameterGenerationOptimizedMeta( checkStepExtension, dataServiceMeta, log );
  }

  private void checkParameterGenerationOptimizedMeta(
      CheckStepsExtension checkStepExtension, DataServiceMeta dataServiceMeta, LogChannelInterface log ) {
    StepMeta checkedStepMeta = checkStepExtension.getStepMetas()[0];

    for ( ParameterGeneration paramGen
      : getParameterGenerationsForStep( dataServiceMeta, checkedStepMeta.getName() ) ) {
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
      StepMeta stepMeta, List<CheckResultInterface> remarks, MongoDbInputMeta mongoDbMeta, VariableSpace space ) {
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
