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


package org.pentaho.di.trans.dataservice.optimization.paramgen;

import org.pentaho.di.trans.step.StepMeta;

public interface ParameterGenerationServiceFactory {

  ParameterGenerationService getService( StepMeta stepMeta );

  boolean supportsStep( StepMeta stepMeta );

}
