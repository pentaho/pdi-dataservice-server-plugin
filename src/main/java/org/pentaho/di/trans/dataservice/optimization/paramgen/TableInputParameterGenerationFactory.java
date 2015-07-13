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

import org.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;

public class TableInputParameterGenerationFactory implements ParameterGenerationServiceFactory {

  @Override
  public ParameterGenerationService getService( StepMeta stepMeta ) {
    return new TableInputParameterGeneration(
      new ValueMetaResolver( getFields( stepMeta.getStepMetaInterface() ) ) );
  }

  @Override
  public boolean supportsStep( StepMeta stepMeta ) {
    StepMetaInterface stepMetaInterface = stepMeta.getStepMetaInterface();
    return stepMetaInterface instanceof TableInputMeta;
  }

  private RowMeta getFields( StepMetaInterface stepMetaInterface ) {
    RowMeta rowMeta = new RowMeta();
    try {
      stepMetaInterface.getFields( rowMeta, "", null, null, null, null, null );
    } catch ( KettleStepException e ) {
      return new RowMeta();
    }
    return rowMeta;
  }
}
