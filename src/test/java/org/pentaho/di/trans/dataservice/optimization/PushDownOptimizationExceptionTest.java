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

package org.pentaho.di.trans.dataservice.optimization;

import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.matchers.JUnitMatchers.*;
import static org.junit.Assert.*;

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
