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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGeneration;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import java.util.Collection;

public class ValidationUtil {

  public static Collection<ParameterGeneration> getParameterGenerationsForStep(
      final DataServiceMeta dataServiceMeta, final String stepName ) {
    return Collections2.transform(
        Collections2.filter( dataServiceMeta.getPushDownOptimizationMeta(), new Predicate<PushDownOptimizationMeta>() {
              @Override
              public boolean apply( PushDownOptimizationMeta pushDownOptimizationMeta ) {
                return pushDownOptimizationMeta.getType() instanceof ParameterGeneration
                    && stepName.equals( pushDownOptimizationMeta.getStepName() );
              }
            }
        ),
        new Function<PushDownOptimizationMeta, ParameterGeneration>() {
          @Override
          public ParameterGeneration apply( PushDownOptimizationMeta pushDownOptimizationMeta ) {
            return (ParameterGeneration) pushDownOptimizationMeta.getType();
          }
        } );
  }

  public static boolean paramSubstitutionModifiesString( String string, VariableSpace space ) {
    return !space.environmentSubstitute( string ).equals( string );
  }

  public static CheckResult warn( String msg, StepMeta stepMeta ) {
    return new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, msg, stepMeta );
  }

  public static CheckResult comment( String msg, StepMeta stepMeta ) {
    return new CheckResult( CheckResultInterface.TYPE_RESULT_COMMENT, msg, stepMeta );
  }

}
