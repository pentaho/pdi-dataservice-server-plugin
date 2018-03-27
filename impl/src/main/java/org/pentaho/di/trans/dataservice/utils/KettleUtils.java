/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.utils;

import org.pentaho.di.core.exception.KettleException;

public class KettleUtils {

  // Singleton pattern for the KettleUtils class
  private static KettleUtils instance = null;

  private KettleUtils() { }

  public static KettleUtils getInstance() {
    if ( instance == null ) {
      instance = new KettleUtils();
    }
    return instance;
  }

  /**
   * Get the value of a specific Kettle Property
   *
   * @param propertyName
   * @return The property value
   * @throws KettleException
   */
  public String getKettleProperty( String propertyName ) throws KettleException {
    // loaded in system properties at startup
    return System.getProperty( propertyName );
  }

}
