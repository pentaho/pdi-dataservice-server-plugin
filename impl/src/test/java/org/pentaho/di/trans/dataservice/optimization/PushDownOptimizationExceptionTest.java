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


package org.pentaho.di.trans.dataservice.optimization;

import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.matchers.JUnitMatchers.containsString;
import static org.junit.Assert.assertThat;

public class PushDownOptimizationExceptionTest {

  @Test public void testConstructors() {
    PushDownOptimizationException verify = new PushDownOptimizationException();
    assertThat( verify.getCause(), nullValue() );

    Throwable cause = new KettleException();
    String message = "Constructor Test";

    verify = new PushDownOptimizationException( message );
    assertThat( verify.getCause(), nullValue() );
    assertThat( verify.getMessage(), containsString( message ) );

    verify = new PushDownOptimizationException( message, cause );
    assertThat( verify.getCause(), equalTo( cause ) );
    assertThat( verify.getMessage(), containsString( message ) );
  }
}
