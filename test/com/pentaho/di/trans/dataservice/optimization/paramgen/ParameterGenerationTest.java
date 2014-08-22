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

package com.pentaho.di.trans.dataservice.optimization.paramgen;

import com.pentaho.di.trans.dataservice.DataServiceMeta;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import com.pentaho.di.trans.dataservice.optimization.SourceTargetFields;
import org.junit.Test;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

public class ParameterGenerationTest {

  public static final String DATA_SERVICE_NAME = "My Data Service";

  @Test
  public void testCRUDFieldMapping() throws Exception {
    ParameterGeneration parameterGeneration = new ParameterGeneration();
    // Get live view of field mappings
    List<SourceTargetFields> fieldMappings = parameterGeneration.getFieldMappings();

    assertEquals( 0, fieldMappings.size() );

    SourceTargetFields mapping = parameterGeneration.createFieldMapping();

    assertEquals( 1, fieldMappings.size() );
    assertTrue( fieldMappings.contains( mapping ) );

    parameterGeneration.removeFieldMapping( mapping );
    assertEquals( 0, fieldMappings.size() );
  }

  private static final String OPT_NAME = "My Optimization";
  private static final String OPT_STEP = "Optimized Step";

  @Test
  public void testSaveLoad() throws Exception {
    MetaStoreFactory<DataServiceMeta> metaStoreFactory = DataServiceMeta
      .getMetaStoreFactory( new MemoryMetaStore(), "Test Namespace" );

    DataServiceMeta expectedDataService = new DataServiceMeta();
    expectedDataService.setName( DATA_SERVICE_NAME );
    expectedDataService.setTransObjectId( UUID.randomUUID().toString() );
    expectedDataService.setStepname( "Service Output Step" );

    PushDownOptimizationMeta expectedOptimization = new PushDownOptimizationMeta( );
    expectedOptimization.setName( OPT_NAME );
    expectedOptimization.setStepName( OPT_STEP );
    expectedDataService.getPushDownOptimizationMeta().add( expectedOptimization );

    ParameterGeneration expectedType = new ParameterGeneration();
    expectedType.setParameterName( "MY_PARAMETER" );
    expectedOptimization.setType( expectedType );

    SourceTargetFields expectedMapping = new SourceTargetFields( "SOURCE", "TARGET" );
    expectedType.getFieldMappings().add( expectedMapping );

    metaStoreFactory.saveElement( expectedDataService );

    List<DataServiceMeta> loadedElements = metaStoreFactory.getElements();
    assertEquals( "No elements loaded", 1, loadedElements.size() );
    DataServiceMeta verifyDataService = metaStoreFactory.loadElement( DATA_SERVICE_NAME );

    assertEquals( expectedDataService.getName(), verifyDataService.getName() );
    assertEquals( expectedDataService.getStepname(), verifyDataService.getStepname() );
    assertEquals( expectedDataService.getTransObjectId(), verifyDataService.getTransObjectId() );

    assertFalse( verifyDataService.getPushDownOptimizationMeta().isEmpty() );
    PushDownOptimizationMeta verifyOptimization = verifyDataService.getPushDownOptimizationMeta().get( 0 );

    assertEquals( expectedOptimization.getName(), verifyOptimization.getName() );
    assertEquals( expectedOptimization.getStepName(), verifyOptimization.getStepName() );

    assertTrue( verifyOptimization.getType() instanceof ParameterGeneration );
    ParameterGeneration verifyType = (ParameterGeneration) verifyOptimization.getType();

    assertEquals( expectedType.getParameterName(), verifyType.getParameterName() );
    assertEquals( expectedType.getForm(), verifyType.getForm() );
    assertEquals( expectedType.getFormName(), OptimizationForm.WHERE_CLAUSE.getFormName() );
    assertFalse( verifyType.getFieldMappings().isEmpty() );
    SourceTargetFields verifyMapping = verifyType.getFieldMappings().get( 0 );

    assertEquals( expectedMapping.getSourceFieldName(), verifyMapping.getSourceFieldName() );
    assertEquals( expectedMapping.getTargetFieldName(), verifyMapping.getTargetFieldName() );
  }
}
