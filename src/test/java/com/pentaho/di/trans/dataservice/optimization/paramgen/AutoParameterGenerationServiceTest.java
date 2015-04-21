/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2015 Pentaho Corporation (Pentaho). All rights reserved.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.pentaho.di.trans.dataservice.DataServiceMeta;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import com.pentaho.di.trans.dataservice.optimization.PushDownType;
import com.pentaho.metaverse.api.ChangeType;
import com.pentaho.metaverse.api.ILineageClient;
import com.pentaho.metaverse.api.StepFieldOperations;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
@SuppressWarnings( "unchecked" )
public class AutoParameterGenerationServiceTest {
  public static final String NAME = "testService";
  public static final String SERVICE_STEP = "Service Step";
  public static final String[] SERVICE_FIELDS = new String[] { "field1", "field2", "field3" };

  @InjectMocks private AutoParameterGenerationService service;

  @Mock private ILineageClient lineageClient;
  @Mock private TransMeta transMeta;
  @Mock private ParameterGenerationFactory serviceProvider;
  private DataServiceMeta dataService;

  @Before
  public void setUp() throws Exception {
    dataService = new DataServiceMeta( NAME, SERVICE_STEP );
    RowMeta serviceRowMeta = mock( RowMeta.class );

    when( transMeta.getStepFields( SERVICE_STEP ) ).thenReturn( serviceRowMeta );
    when( serviceRowMeta.getFieldNames() ).thenReturn( SERVICE_FIELDS );
  }

  @Test
  public void testApply() throws Exception {
    Map<String, Set<List<StepFieldOperations>>> operationPaths;
    List<StepFieldOperations> field1_input1 = Lists.newArrayList(
      new StepFieldOperations( "Input 1", "field1_origin", null ),
      new StepFieldOperations( "Modifier", "field1", new MockOperations().
        put( ChangeType.DATA ).put( ChangeType.METADATA )
      )
    );
    List<StepFieldOperations> field2_input1 = Lists.newArrayList(
      new StepFieldOperations( "Input 1", "field2_origin", null ),
      new StepFieldOperations( "Rename", "field2", new MockOperations().
        put( ChangeType.METADATA )
      )
    );
    List<StepFieldOperations> field2_input2 = Lists.newArrayList(
      new StepFieldOperations( "Input 2", "field2", null )
    );

    operationPaths = Maps.newHashMap();
    operationPaths.put( "field1", ImmutableSet.of( field1_input1 ) );
    operationPaths.put( "field2", ImmutableSet.of( field2_input1, field2_input2 ) );

    when( lineageClient.getOperationPaths(
        same( transMeta ), eq( SERVICE_STEP ),
        eq( ImmutableList.copyOf( SERVICE_FIELDS ) ) )
    ).thenReturn( operationPaths );

    StepMeta input1Meta = mock( StepMeta.class );
    when( transMeta.findStep( "Input 1" ) ).thenReturn( input1Meta );
    when( serviceProvider.supportsStep( input1Meta ) ).thenReturn( true );

    List<PushDownOptimizationMeta> optimizationMetaList = service.apply( transMeta, dataService );
    assertThat( optimizationMetaList, contains( allOf(
      hasProperty( "stepName", equalTo( "Input 1" ) ),
      hasProperty( "type", hasProperty( "fieldMappings", contains(
          hasProperty( "targetFieldName", equalTo( "field2" ) ) )
      ) )
    ) ) );
  }

  @Test
  public void testApplyWithExisting() throws Exception {
    Map<String, Set<List<StepFieldOperations>>> operationPaths;
    List<StepFieldOperations> inputOperations = ImmutableList.of(
      new StepFieldOperations( "Input", "field", null )
    );
    operationPaths = ImmutableMap.of( "Input 1", Collections.singleton( inputOperations ) );

    when( lineageClient.getOperationPaths(
        same( transMeta ), eq( SERVICE_STEP ),
        eq( ImmutableList.copyOf( SERVICE_FIELDS ) ) )
    ).thenReturn( operationPaths );

    StepMeta input1Meta = mock( StepMeta.class );
    when( transMeta.findStep( "Input" ) ).thenReturn( input1Meta );
    when( serviceProvider.supportsStep( input1Meta ) ).thenReturn( true );

    assertThat( service.parametrizedSteps( dataService ), empty() );
    assertThat( service.apply( transMeta, dataService ), hasSize( 1 ) );

    PushDownOptimizationMeta optimizationMeta = new PushDownOptimizationMeta();
    optimizationMeta.setName( "Existing PDO" );
    optimizationMeta.setStepName( "Input" );
    optimizationMeta.setType( new ParameterGeneration() );
    dataService.setPushDownOptimizationMeta( Lists.newArrayList( optimizationMeta ) );

    assertThat( service.parametrizedSteps( dataService ), contains( "Input" ) );
    assertThat( service.apply( transMeta, dataService ), empty() );
  }

  @Test
  public void testProvidedOptimizationTypes() throws Exception {
    Set<Class<? extends PushDownType>> expected = Sets.newHashSet();
    expected.add( ParameterGeneration.class );

    assertThat( service.getProvidedOptimizationTypes(), equalTo( expected ) );
  }
}
