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

package org.pentaho.di.trans.dataservice.streaming;

import com.google.common.base.Objects;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.trans.dataservice.optimization.OptimizationImpactInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * {@link StreamServiceKey} test class
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class StreamServiceKeyTest {
  private String mockDataServiceId;
  private String mockOtherDataServiceId;
  private Map<String, String> mockParameters;
  private Map<String, String> mockOtherParameters;
  private List<OptimizationImpactInfo> mockOptimizationList;
  private List<OptimizationImpactInfo> mockOtherOptimizationList;
  private String queryAfterOptimization;
  private String otherQueryAfterOptimization;

  @Mock
  OptimizationImpactInfo mockOptimizationImpact;

  @Mock
  OptimizationImpactInfo mockEmptyOptimizationImpact;

  @Mock
  OptimizationImpactInfo mockNullOptimizationImpact;

  @Mock
  OptimizationImpactInfo mockOtherOptimizationImpact;

  @Before
  public void setup() throws Exception {
    mockDataServiceId = "Mock ID";
    mockParameters = new HashMap();
    mockOtherDataServiceId = "Mock ID";
    mockOtherParameters = new HashMap();
    queryAfterOptimization = "Mock Optimized Query";
    otherQueryAfterOptimization = "Mock Other Optimized Query";

    when( mockOptimizationImpact.getQueryAfterOptimization() ).thenReturn( queryAfterOptimization );
    when( mockEmptyOptimizationImpact.getQueryAfterOptimization() ).thenReturn( "" );
    when( mockNullOptimizationImpact.getQueryAfterOptimization() ).thenReturn( null );
    when( mockOtherOptimizationImpact.getQueryAfterOptimization() ).thenReturn( otherQueryAfterOptimization );

    mockOptimizationList = new ArrayList<>(  );
    mockOptimizationList.add( mockOptimizationImpact );
    mockOptimizationList.add( mockEmptyOptimizationImpact );
    mockOptimizationList.add( mockNullOptimizationImpact );

    mockOtherOptimizationList = new ArrayList<>(  );
    mockOtherOptimizationList.add( mockOtherOptimizationImpact );
  }

  @Test
  public void testCreate() {
    StreamServiceKey key = StreamServiceKey.create( mockDataServiceId, mockParameters, mockOptimizationList );

    assertNotNull( key );
    assertEquals( mockDataServiceId, key.getDataServiceId() );
    assertEquals( mockParameters, key.getParameters() );
    assertNotSame( mockParameters, key.getParameters() );
    assertEquals( 1, key.getOptimizations().size() );
    assertEquals( queryAfterOptimization, key.getOptimizations().get( 0 ) );
  }

  @Test
  public void testEquals() {
    StreamServiceKey key = StreamServiceKey.create( mockDataServiceId, mockParameters, mockOptimizationList );
    StreamServiceKey key2 = StreamServiceKey.create( mockOtherDataServiceId, mockOtherParameters,
      mockOptimizationList );

    assertNotNull( key );
    assertNotNull( key2 );

    assertTrue( key.equals( key2 ) );
    assertTrue( key2.equals( key ) );

    mockParameters.put( "Test", "Test" );

    key = StreamServiceKey.create( mockDataServiceId, mockParameters, mockOptimizationList );

    assertFalse( key.equals( key2 ) );
    assertFalse( key2.equals( key ) );

    mockOtherParameters.put( "Test", "Test" );

    key2 = StreamServiceKey.create( mockOtherDataServiceId, mockOtherParameters, mockOptimizationList );

    assertTrue( key.equals( key2 ) );
    assertTrue( key2.equals( key ) );

    mockDataServiceId = mockDataServiceId + mockDataServiceId;

    key = StreamServiceKey.create( mockDataServiceId, mockParameters, mockOptimizationList );

    assertFalse( key.equals( key2 ) );
    assertFalse( key2.equals( key ) );

    mockOtherDataServiceId = mockDataServiceId;

    key2 = StreamServiceKey.create( mockOtherDataServiceId, mockOtherParameters, mockOptimizationList );

    assertTrue( key.equals( key2 ) );
    assertTrue( key2.equals( key ) );

    key2 = StreamServiceKey.create( mockOtherDataServiceId, mockOtherParameters, mockOtherOptimizationList );

    assertFalse( key.equals( key2 ) );
    assertFalse( key2.equals( key ) );

    key = StreamServiceKey.create( mockDataServiceId, mockParameters, mockOtherOptimizationList );

    assertTrue( key.equals( key2 ) );
    assertTrue( key2.equals( key ) );
  }

  @Test
  public void testHashCode() {
    StreamServiceKey key = StreamServiceKey.create( mockDataServiceId, mockParameters, mockOptimizationList );
    List<String> optimizations = new ArrayList<>();
    optimizations.add( queryAfterOptimization );
    int hashCode = Objects.hashCode( mockDataServiceId, mockParameters, optimizations );
    assertEquals( hashCode, key.hashCode() );
  }

  @Test
  public void testToString() {
    StreamServiceKey key = StreamServiceKey.create( mockDataServiceId, mockParameters, mockOptimizationList );
    String toStringTest = "StreamServiceKey{dataServiceId="
      + mockDataServiceId
      + ", parameters="
      + mockParameters.toString()
      + ", optimizations=["
      + queryAfterOptimization
      + "]}";
    assertEquals( toStringTest, key.toString() );

    mockParameters.put( "Test", "Test" );
    toStringTest = "StreamServiceKey{dataServiceId="
      + mockDataServiceId
      + ", parameters="
      + mockParameters.toString()
      + ", optimizations=["
      + queryAfterOptimization
      + "]}";

    key = StreamServiceKey.create( mockDataServiceId, mockParameters, mockOptimizationList );
    assertEquals( toStringTest, key.toString() );
  }

  @Test
  public void testImmutable(){
    Map<String, String> parameters = new HashMap<>( );
    parameters.put( "key1", "value1" );
    List<OptimizationImpactInfo> optimizations = new ArrayList<>();
    OptimizationImpactInfo optimizationImpactInfo = new OptimizationImpactInfo( "STEP_NAME" );
    optimizationImpactInfo.setQueryAfterOptimization( "AFTER" );
    optimizations.add( optimizationImpactInfo );

    StreamServiceKey streamServiceKey = StreamServiceKey.create( "DATA_SERVICE_ID", parameters, optimizations );
    int streamServiceKeyValue = streamServiceKey.hashCode();
    StreamServiceKey otherStreamServiceKey = StreamServiceKey.create( "DATA_SERVICE_ID", parameters, optimizations );

    assertEquals( streamServiceKeyValue, otherStreamServiceKey.hashCode() );
    assertEquals( 1, streamServiceKey.getParameters().size() );
    assertEquals( Arrays.asList("AFTER"), streamServiceKey.getOptimizations() );
    assertEquals( "DATA_SERVICE_ID", streamServiceKey.getDataServiceId() );

    parameters.clear();
    parameters.put( "key2", "value2" );

    assertEquals( streamServiceKeyValue, streamServiceKey.hashCode() );
    assertEquals( 1, streamServiceKey.getParameters().size() );

    OptimizationImpactInfo optimizationImpactInfo2 = new OptimizationImpactInfo( "STEP_NAME_2" );
    optimizationImpactInfo2.setQueryAfterOptimization( "AFTER2" );
    optimizations.add( optimizationImpactInfo2 );

    optimizationImpactInfo.setQueryAfterOptimization( "AFTER_CHANGED" );

    assertEquals( streamServiceKeyValue, streamServiceKey.hashCode() );
    assertEquals( 1, streamServiceKey.getParameters().size() );
    assertEquals( Arrays.asList("AFTER"), streamServiceKey.getOptimizations() );
    assertEquals( "DATA_SERVICE_ID", streamServiceKey.getDataServiceId() );
  }
}
