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
