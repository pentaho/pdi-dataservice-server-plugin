/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package com.pentaho.di.trans.dataservice.optimization;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class PushDownOptimizationMetaTest {

  private static final String NAME = "Test Optimization Name";
  private static final String STEP = "Test Step Name";
  public static final Map<String, String> TYPE_CONTEXT = ImmutableMap.of( "param1", "value1", "param2", "value2" );
  public static final String EXTRA_PARAM = "Type attribute";

  @Test
  /**
   * Integration test, verifies persistence of optimization meta through MetaStore
   * Save-load from MemoryMetaStore
   */
  public void testSaveLoad() throws Exception {
    MetaStoreFactory<PushDownOptimizationMeta> metaStoreFactory = PushDownOptimizationMeta
      .getMetaStoreFactory( new MemoryMetaStore(), "Test Namespace" );

    PushDownOptimizationMeta expected = new PushDownOptimizationMeta( );
    TestType expectedType = new TestType();

    expected.setName( NAME );
    expected.setStepName( STEP );
    expected.setType( expectedType );
    expectedType.setExtraParam( EXTRA_PARAM );
    expectedType.loadParameters( TYPE_CONTEXT );

    metaStoreFactory.saveElement( expected );

    List<PushDownOptimizationMeta> loadedElements = metaStoreFactory.getElements();
    assertEquals( "No elements loaded", 1, loadedElements.size() );
    PushDownOptimizationMeta verify = loadedElements.get( 0 );

    assertEquals( "Round trip: Name", NAME, verify.getName() );
    assertEquals( "Round trip: Step Name", STEP, verify.getStepName() );
    assertEquals( "Round trip: Type", expectedType, verify.getType() );
  }

  public static class TestType implements PushDownType {
    private Map<String, String> params;

    @MetaStoreAttribute
    private String extraParam;

    @Override public String getTypeName() {
      return "Test Optimization";
    }

    @Override public String getFormName() {
      return "";
    }

    @Override public Map<String, String> saveParameters() {
      return params;
    }

    @Override public void loadParameters( Map<String, String> params ) {
      this.params = params;
    }

    public String getExtraParam() {
      return extraParam;
    }

    public void setExtraParam( String extraParam ) {
      this.extraParam = extraParam;
    }

    @Override public boolean equals( Object obj ) {
      if ( obj instanceof TestType ) {
        final TestType that = (TestType) obj;
        return Objects.equals( this.extraParam, that.extraParam ) && Objects.equals( this.params, that.params );
      } else {
        return false;
      }
    }

    @Override public String toString() {
      return getTypeName() + ": extraParam=" + extraParam + "| params=" + params;
    }
  }

}
