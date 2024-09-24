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

package org.pentaho.di.trans.dataservice.ui.model;

import org.junit.Assert;
import org.junit.Test;

public class DataServiceRemapStepChooserModelTest {
  @Test
  public void testAccessors() {
    final String STEP_NAME = "step1";
    DataServiceRemapStepChooserModel model = new DataServiceRemapStepChooserModel();
    model.setServiceStep( STEP_NAME );
    Assert.assertEquals( STEP_NAME, model.getServiceStep() );
  }
}
