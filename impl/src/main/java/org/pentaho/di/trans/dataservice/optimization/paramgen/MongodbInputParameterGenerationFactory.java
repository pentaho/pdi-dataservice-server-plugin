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

import org.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

public class MongodbInputParameterGenerationFactory implements ParameterGenerationServiceFactory {

  @Override
  public ParameterGenerationService getService( StepMeta stepMeta ) {
    return new MongodbInputParameterGeneration(
      new ValueMetaResolver( getFields( stepMeta.getStepMetaInterface() ) ) );
  }

  @Override
  public boolean supportsStep( StepMeta stepMeta ) {
    return "MongoDbInput".equals( stepMeta.getTypeId() );
  }

  private RowMeta getFields( StepMetaInterface stepMetaInterface ) {
    RowMeta rowMeta = new RowMeta();
    try {
      // MongoDbInputMeta only implements the deprecated version of .getFields().
      stepMetaInterface.getFields( rowMeta, "", null, null, null );
    } catch ( KettleStepException e ) {
      return new RowMeta();
    }
    return rowMeta;
  }

}
