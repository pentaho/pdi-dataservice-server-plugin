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
   * @param propertyName The name of the property to retrieve
   * @return The property value
   * @throws KettleException
   */
  public String getKettleProperty( String propertyName ) throws KettleException  {
    return getKettleProperty( propertyName, "" );
  }

  /**
   * Get the value of a specific Kettle Property or a default value if present
   *
   * @param propertyName The name of the property to retrieve
   * @param defaultValue An optional default value in case the property is not found
   * @return The property value
   * @throws KettleException
   */
  public String getKettleProperty( String propertyName, String defaultValue ) throws KettleException {
    // loaded in system properties at startup
    if ( defaultValue != null && !defaultValue.isEmpty() ) {
      return System.getProperty( propertyName, defaultValue );
    } else {
      return System.getProperty( propertyName );
    }
  }

}
