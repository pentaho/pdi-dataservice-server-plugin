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

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KettleUtilsTest {
  private String VALID_PROPERTY = "valid_property_value";
  private KettleUtils kettleUtils = KettleUtils.getInstance();

  @Before
  public void setUp() {
    System.setProperty( "VALID_PROPERTY", String.valueOf( VALID_PROPERTY ) );
  }

  @Test
  public void testGettingExistingProperty() throws KettleException {
    assertEquals( kettleUtils.getKettleProperty(  "VALID_PROPERTY" ),
        VALID_PROPERTY );
  }

  @Test
  public void testGettingMissingProperty() throws KettleException {
    assertEquals( kettleUtils.getKettleProperty(  "INVALID_PROPERTY" ),null );
  }

  @Test
  public void testGettingExistingPropertyWithDefault() throws KettleException {
    assertEquals( kettleUtils.getKettleProperty(  "VALID_PROPERTY", "someDefaultValue" ),
        VALID_PROPERTY );
  }

  @Test
  public void testGettingMissingPropertyWithDefault() throws KettleException {
    assertEquals( kettleUtils.getKettleProperty(  "INVALID_PROPERTY", "someDefaultValue" ),
        "someDefaultValue" );
  }

  @Test
  public void testGettingExistingPropertyWithNullDefault() throws KettleException {
    assertEquals( kettleUtils.getKettleProperty(  "VALID_PROPERTY", null ),
        VALID_PROPERTY );
  }

  @Test
  public void testGettingExistingPropertyWithEmptyDefault() throws KettleException {
    assertEquals( kettleUtils.getKettleProperty(  "VALID_PROPERTY", "" ),
        VALID_PROPERTY );
  }

  @Test
  public void testGettingMissingPropertyWithNullDefault() throws KettleException {
    assertEquals( kettleUtils.getKettleProperty(  "INVALID_PROPERTY", null ),
        null );
  }

  @Test
  public void testGettingMissingPropertyWithEmptyDefault() throws KettleException {
    assertEquals( kettleUtils.getKettleProperty(  "INVALID_PROPERTY", "" ),
        null );
  }

  @Test( expected = KettleException.class )
  public void testGettingWithException() throws KettleException {
    KettleUtils ku = mock( KettleUtils.class );

    when( ku.getKettleProperty( "VALID_PROPERTY" ) ).thenThrow( new KettleException() );
    ku.getKettleProperty(  "VALID_PROPERTY" );
  }
}
