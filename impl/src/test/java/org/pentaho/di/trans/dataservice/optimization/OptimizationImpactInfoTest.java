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
