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
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.CheckStepsExtension;
import org.pentaho.di.trans.step.StepMeta;

public interface StepValidation {

  public boolean supportsStep( StepMeta step, LogChannelInterface log );

  public void checkStep(
      CheckStepsExtension checkStepExtension, DataServiceMeta dataServiceMeta, LogChannelInterface log );

}
