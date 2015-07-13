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
