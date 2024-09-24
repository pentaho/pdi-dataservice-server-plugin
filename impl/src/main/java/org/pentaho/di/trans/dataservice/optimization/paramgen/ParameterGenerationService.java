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
