/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataServiceMetaTest extends BaseTest {

  @Test
  public void testSaveUserDefined() {
    try {
      transMeta.setAttribute( DATA_SERVICE_STEP, DataServiceMeta.IS_USER_DEFINED,
          ( dataService.isUserDefined() ? "Y" : "N" ) );
      String xml = transMeta.getXML();
      assertTrue( xml.indexOf( "<key>is_user_defined</key>" + System.getProperty("line.separator")
          + "        <value>Y</value>" ) > 0 );
    } catch( Exception ex ){
      fail();
    }
  }

  @Test
  public void testSaveTransient() {
    try {
      dataService.setUserDefined( false );
      transMeta.setAttribute( DATA_SERVICE_STEP, DataServiceMeta.IS_USER_DEFINED,
          ( dataService.isUserDefined() ? "Y" : "N" ) );
      String xml = transMeta.getXML();
      assertTrue( xml.indexOf( "<key>is_user_defined</key>" + System.getProperty("line.separator")
          + "        <value>N</value>" ) > 0 );
    } catch( Exception ex ){
      fail();
    }
  }
}
