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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.TransMeta;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.RETURNS_DEFAULTS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AutoParameterGenerationServiceTest {
  public static final String NAME = "testService";
  public static final String SERVICE_STEP = "Service Step";
  public static final List<String> SERVICE_FIELDS = ImmutableList.of("field1", "field2", "field3");
  AutoParameterGenerationService service;
  ILineageClient lineageClient;
  private ParameterGenerationFactory serviceProvider;

  @Before
  public void setUp() throws Exception {
    lineageClient = mock( ILineageClient.class );
    serviceProvider = mock( ParameterGenerationFactory.class );
    service = new AutoParameterGenerationService( lineageClient, serviceProvider );
  }

  @Test
  public void testApply() throws Exception {
    // Prepare mocks
    DataServiceMeta dataServiceMeta = new DataServiceMeta( NAME, SERVICE_STEP, false, false, null );

    TransMeta transMeta = mock( TransMeta.class );
    RowMeta rowMeta = new RowMeta();
    for ( String serviceField : SERVICE_FIELDS ) {
      rowMeta.addValueMeta( new ValueMeta( serviceField, ValueMetaInterface.TYPE_STRING ) );
    }
    when( transMeta.getStepFields( SERVICE_STEP ) ).thenReturn( rowMeta );

    SourceLineageMap sourceLineageMap = mock( SourceLineageMap.class, new Answer() {
      @Override public Object answer( InvocationOnMock invocationOnMock ) throws Throwable {
        // Return self (fluent-ish interface)
        Object mock = invocationOnMock.getMock();
        if ( invocationOnMock.getMethod().getReturnType().isInstance( mock ) ) {
          return mock;
        } else {
          return RETURNS_DEFAULTS.answer( invocationOnMock );
        }
      }
    } );

    AutoParameterGenerationService spy = spy( service );
    Map<String, Set<List<StepFieldOperations>>> operationPaths = Maps.newHashMap();
    List<PushDownOptimizationMeta> expectedOptimizations = Lists.newArrayList();

    when( lineageClient.getOperationPaths( transMeta, SERVICE_STEP, SERVICE_FIELDS ) ).thenReturn( operationPaths );
    doReturn( sourceLineageMap ).when( spy ).convertToTable( same( operationPaths ) );
    when( sourceLineageMap.generateOptimizationList() ).thenReturn( expectedOptimizations );

    // Execute apply
    List<PushDownOptimizationMeta> pushDownOptimizations = spy.apply( transMeta, dataServiceMeta );

    // Verify
    assertThat( pushDownOptimizations, sameInstance( expectedOptimizations ) );

    verify( sourceLineageMap ).filterChangedValues();
    verify( sourceLineageMap ).filterExistingOptimizations( dataServiceMeta );
    verify( sourceLineageMap ).filterSupportedInputs( transMeta, serviceProvider );
    verify( sourceLineageMap ).generateOptimizationList();
    verifyNoMoreInteractions( sourceLineageMap );
  }

  @Test
  public void testConvertToTable() throws Exception {
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

    SourceLineageMap table = service.convertToTable( operationPaths );

    SourceLineageMap expected = new SourceLineageMap(
      ImmutableSetMultimap.<String, List<StepFieldOperations>>builder().
        put( "Input 1", field1_input1 ).
        put( "Input 1", field2_input1 ).
        put( "Input 2", field2_input2 ).
        build()
    );
    assertThat( table, equalTo( expected ) );
  }

  @Test
  public void testProvidedOptimizationTypes() throws Exception {
    Set<Class<? extends PushDownType>> expected = Sets.newHashSet();
    expected.add( ParameterGeneration.class );

    assertThat( service.getProvidedOptimizationTypes(), equalTo( expected ) );
  }
}
