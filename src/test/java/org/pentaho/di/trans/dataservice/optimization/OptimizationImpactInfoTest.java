/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

public class OptimizationImpactInfoTest {

  @Test
  public void checkDescriptionWithoutError() throws Exception {
    OptimizationImpactInfo info = new OptimizationImpactInfo( "test" );

    assertEquals( info.getErrorMsg(), "" );                     // value after initialization for errorMsg
    assertFalse( info.getDescription().contains( "[ERROR]" ) ); // message without errors
  }

  @Test
  public void checkDescriptionWithNotNullErrorMessage() throws Exception {
    OptimizationImpactInfo info = new OptimizationImpactInfo( "test" );

    info.setErrorMsg( "The detail message of error" );          //if error
    assertThat( info.getDescription(), CoreMatchers.containsString( "The detail message of error" ) );

    info.setErrorMsg( new Exception( "The detail message of error" ) );          //if error
    assertThat( info.getDescription(), CoreMatchers.containsString( "The detail message of error" ) );
  }

  @Test
  public void checkDescriptionWithNullErrorMessage() throws Exception {
    OptimizationImpactInfo info = new OptimizationImpactInfo( "test" );

    info.setErrorMsg( (String) null );                          //if the detail message string is null
    assertThat( info.getDescription(), CoreMatchers.containsString( "[ERROR]" ) );

    info.setErrorMsg( new NullPointerException() );             //if the detail message string is null
    assertThat( info.getDescription(), CoreMatchers.containsString( "[ERROR]" ) );
  }
}
