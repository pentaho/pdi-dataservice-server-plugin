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

package org.pentaho.di.trans.dataservice.streaming;

import com.google.common.base.Objects;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * {@link StreamServiceKey} test class
 */
@RunWith( MockitoJUnitRunner.class )
public class StreamServiceKeyTest {
  private String mockDataServiceId;
  private String mockOtherDataServiceId;
  private Map<String, String> mockParameters;
  private Map<String, String> mockOtherParameters;

  @Before
  public void setup() throws Exception {
    mockDataServiceId = "Mock ID";
    mockParameters = new HashMap();
    mockOtherDataServiceId = "Mock ID";
    mockOtherParameters = new HashMap();
  }

  @Test
  public void testCreate() {
    StreamServiceKey key = StreamServiceKey.create( mockDataServiceId, mockParameters );

    assertNotNull( key );
    assertEquals( mockDataServiceId, key.getDataServiceId() );
    assertEquals( mockParameters, key.getParameters() );
    assertNotSame( mockParameters, key.getParameters() );
  }

  @Test
  public void testEquals() {
    StreamServiceKey key = StreamServiceKey.create( mockDataServiceId, mockParameters );
    StreamServiceKey key2 = StreamServiceKey.create( mockOtherDataServiceId, mockOtherParameters );

    assertNotNull( key );
    assertNotNull( key2 );

    assertTrue( key.equals( key2 ) );
    assertTrue( key2.equals( key ) );

    mockParameters.put( "Test", "Test" );

    key = StreamServiceKey.create( mockDataServiceId, mockParameters );

    assertFalse( key.equals( key2 ) );
    assertFalse( key2.equals( key ) );

    mockOtherParameters.put( "Test", "Test" );

    key2 = StreamServiceKey.create( mockOtherDataServiceId, mockOtherParameters );

    assertTrue( key.equals( key2 ) );
    assertTrue( key2.equals( key ) );

    mockDataServiceId = mockDataServiceId + mockDataServiceId;

    key = StreamServiceKey.create( mockDataServiceId, mockParameters );

    assertFalse( key.equals( key2 ) );
    assertFalse( key2.equals( key ) );

    mockOtherDataServiceId = mockDataServiceId;

    key2 = StreamServiceKey.create( mockOtherDataServiceId, mockOtherParameters );

    assertTrue( key.equals( key2 ) );
    assertTrue( key2.equals( key ) );

    assertTrue( key.equals( key ) );
    assertFalse( key.equals( null ) );
  }

  @Test
  public void testHashCode() {
    StreamServiceKey key = StreamServiceKey.create( mockDataServiceId, mockParameters );
    int hashCode = Objects.hashCode( mockDataServiceId, mockParameters );
    assertEquals( hashCode, key.hashCode() );
  }

  @Test
  public void testToString() {
    StreamServiceKey key = StreamServiceKey.create( mockDataServiceId, mockParameters );
    String toStringTest = "StreamServiceKey{dataServiceId="
      + mockDataServiceId
      + ", parameters="
      + mockParameters.toString()
      + "}";
    assertEquals( toStringTest, key.toString() );

    mockParameters.put( "Test", "Test" );
    toStringTest = "StreamServiceKey{dataServiceId="
      + mockDataServiceId
      + ", parameters="
      + mockParameters.toString()
      + "}";

    key = StreamServiceKey.create( mockDataServiceId, mockParameters );
    assertEquals( toStringTest, key.toString() );
  }
}
