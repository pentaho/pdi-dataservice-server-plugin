/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.optimization.paramgen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownType;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metaverse.api.ChangeType;
import org.pentaho.metaverse.api.ILineageClient;
import org.pentaho.metaverse.api.StepFieldOperations;

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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.StrictStubs.class)
@SuppressWarnings( "unchecked" )
public class AutoParameterGenerationServiceTest {
  public static final String NAME = "testService";
  public static final String SERVICE_STEP = "Service Step";
  public static final String[] SERVICE_FIELDS = new String[] { "field1", "field2", "field3" };

  @InjectMocks private AutoParameterGenerationService service;

  @Mock private ILineageClient lineageClient;
  @Mock private TransMeta transMeta;
  @Mock private ParameterGenerationFactory serviceProvider;
  @Mock private ParameterGeneration parameterGeneration;
  private DataServiceMeta dataService;

  @Before
  public void setUp() throws Exception {
    dataService = new DataServiceMeta( transMeta );
    dataService.setName( NAME );
    dataService.setStepname( SERVICE_STEP );
    RowMeta serviceRowMeta = mock( RowMeta.class );

    when( transMeta.getStepFields( SERVICE_STEP ) ).thenReturn( serviceRowMeta );
    when( transMeta.getLogChannel() ).thenReturn( mock( LogChannel.class ) );
    when( serviceRowMeta.getFieldNames() ).thenReturn( SERVICE_FIELDS );
    when( serviceProvider.createPushDown() ).thenReturn( parameterGeneration );
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
    lenient().when( transMeta.findStep( "Input 1" ) ).thenReturn( input1Meta );
    when( serviceProvider.supportsStep( input1Meta ) ).thenReturn( true );

    List<PushDownOptimizationMeta> optimizationMetaList = service.apply( dataService );
    assertThat( optimizationMetaList, contains( allOf(
      hasProperty( "stepName", equalTo( "Input 1" ) ),
      hasProperty( "type", is( parameterGeneration ) )
    ) ) );
    verify( parameterGeneration ).setParameterName( anyString() );
    verify( parameterGeneration ).createFieldMapping( "field2", "field2_origin" );
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
    assertThat( service.apply( dataService ), hasSize( 1 ) );

    PushDownOptimizationMeta optimizationMeta = new PushDownOptimizationMeta();
    optimizationMeta.setName( "Existing PDO" );
    optimizationMeta.setStepName( "Input" );
    optimizationMeta.setType( mock( ParameterGeneration.class ) );
    dataService.setPushDownOptimizationMeta( Lists.newArrayList( optimizationMeta ) );

    assertThat( service.parametrizedSteps( dataService ), contains( "Input" ) );
    assertThat( service.apply( dataService ), empty() );
  }

  @Test
  public void testProvidedOptimizationTypes() throws Exception {
    Set<Class<? extends PushDownType>> expected = Sets.newHashSet();
    expected.add( ParameterGeneration.class );

    assertThat( service.getProvidedOptimizationTypes(), equalTo( expected ) );
  }
}
