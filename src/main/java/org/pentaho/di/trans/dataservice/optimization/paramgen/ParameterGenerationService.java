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

package org.pentaho.di.trans.dataservice.optimization.paramgen;

import org.pentaho.di.trans.dataservice.optimization.OptimizationImpactInfo;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationException;
import org.pentaho.di.core.Condition;
import org.pentaho.di.trans.step.StepInterface;

/**
 * @author nhudak
 */
public interface ParameterGenerationService {
  public void pushDown( Condition condition, ParameterGeneration parameterGeneration, StepInterface stepInterface ) throws PushDownOptimizationException;

  public String getParameterDefault();

  OptimizationImpactInfo preview( Condition pushDownCondition, ParameterGeneration parameterGeneration, StepInterface stepInterface );
}
