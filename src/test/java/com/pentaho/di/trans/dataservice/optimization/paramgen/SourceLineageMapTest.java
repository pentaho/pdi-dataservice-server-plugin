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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.pentaho.di.trans.dataservice.DataServiceMeta;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import com.pentaho.di.trans.dataservice.optimization.SourceTargetFields;
import com.pentaho.metaverse.analyzer.kettle.ChangeType;
import com.pentaho.metaverse.client.StepFieldOperations;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import java.util.List;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SourceLineageMapTest {

  private SourceLineageMap table;

  @Before
  public void setUp() throws Exception {
    table = SourceLineageMap.create();
  }

  @Test
  public void testFilterChangedValues() throws Exception {
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
    table.put( "Input 1", field1_input1 );
    table.put( "Input 1", field2_input1 );

    table.filterChangedValues();

    assertThat( table, equalTo( new SourceLineageMap( ImmutableSetMultimap.of( "Input 1", field2_input1 ) ) ) );
  }

  @Test
  public void testFilterSupportedInputs() throws Exception {
    TransMeta transMeta = mock( TransMeta.class );
    StepMeta supportedInput = mock( StepMeta.class );
    when( transMeta.findStep( "Supported Input" ) ).thenReturn( supportedInput );
    StepMeta unsupportedInput = mock( StepMeta.class );
    when( transMeta.findStep( "Unsupported Input" ) ).thenReturn( unsupportedInput );

    ParameterGenerationServiceProvider serviceProvider = mock( ParameterGenerationServiceProvider.class );
    when( serviceProvider.supportsStep( supportedInput ) ).thenReturn( true );
    when( serviceProvider.supportsStep( unsupportedInput ) ).thenReturn( false );

    List<StepFieldOperations> field1_supported = Lists.newArrayList(
      new StepFieldOperations( "Supported Input", "field1", null )
    );
    List<StepFieldOperations> field1_unsupported = Lists.newArrayList(
      new StepFieldOperations( "Unsupported Input", "field1", null )
    );
    table.put( "Supported Input", field1_supported );
    table.put( "Unsupported Input", field1_unsupported );

    table.filterSupportedInputs( transMeta, serviceProvider );

    assertThat( table,
      equalTo( new SourceLineageMap( ImmutableSetMultimap.of( "Supported Input", field1_supported ) ) ) );
  }

  @Test
  public void testFilterExistingOptimizations() throws Exception {
    DataServiceMeta dataServiceMeta = new DataServiceMeta();
    dataServiceMeta.setName( "Data Service" );
    dataServiceMeta.setStepname( "Service Step" );
    PushDownOptimizationMeta existing = new PushDownOptimizationMeta();
    existing.setName( "Custom Optimization" );
    existing.setStepName( "Input 2" );
    existing.setType( new ParameterGeneration() );
    dataServiceMeta.getPushDownOptimizationMeta().add( existing );

    List<StepFieldOperations> field1_input1 = Lists.newArrayList(
      new StepFieldOperations( "Input 1", "field1", null )
    );
    List<StepFieldOperations> field1_input2 = Lists.newArrayList(
      new StepFieldOperations( "Input 2", "field1", null )
    );
    table.put( "Input 1", field1_input1 );
    table.put( "Input 2", field1_input2 );

    table.filterExistingOptimizations( dataServiceMeta );

    assertThat( table,
      equalTo( new SourceLineageMap( ImmutableSetMultimap.of( "Input 1", field1_input1 ) ) ) );
  }

  @Test
  public void testGenerateOptimizationList() throws Exception {
    List<StepFieldOperations> field1_input1 = Lists.newArrayList(
      new StepFieldOperations( "Input 1", "field1", null )
    );
    List<StepFieldOperations> field2_input1 = Lists.newArrayList(
      new StepFieldOperations( "Input 1", "field2_origin", null ),
      new StepFieldOperations( "Meta modifier", "field2", new MockOperations().put( ChangeType.METADATA ) )
    );
    List<StepFieldOperations> field1_input2 = Lists.newArrayList(
      new StepFieldOperations( "Input 2", "field1", null )
    );
    table.put( "Input 1", field1_input1 );
    table.put( "Input 1", field2_input1 );
    table.put( "Input 2", field1_input2 );

    // Execute
    List<PushDownOptimizationMeta> optimizationList = table.generateOptimizationList();
    assertThat( optimizationList.size(), equalTo( 2 ) );

    // Input 1 Optimization
    PushDownOptimizationMeta input1Opt = Iterables.find( optimizationList, new Predicate<PushDownOptimizationMeta>() {
      @Override public boolean apply( PushDownOptimizationMeta input ) {
        return "Input 1".equals( input.getStepName() );
      }
    } );
    assertThat( input1Opt.getName(), containsString( "Input 1" ) );
    assertThat( input1Opt.getType(), instanceOf( ParameterGeneration.class ) );
    List<SourceTargetFields> input1Mappings = ( (ParameterGeneration) input1Opt.getType() ).getFieldMappings();
    assertThat( input1Mappings, hasItem( allOf(
      isA( SourceTargetFields.class ),
      hasProperty( "sourceFieldName", equalTo( "field1" ) ),
      hasProperty( "targetFieldName", equalTo( "field1" ) )
    ) ) );
    assertThat( input1Mappings, hasItem( allOf(
      isA( SourceTargetFields.class ),
      hasProperty( "sourceFieldName", equalTo( "field2_origin" ) ),
      hasProperty( "targetFieldName", equalTo( "field2" ) )
    ) ) );

    // Input 2 Optimization
    PushDownOptimizationMeta input2Opt = Iterables.find( optimizationList, new Predicate<PushDownOptimizationMeta>() {
      @Override public boolean apply( PushDownOptimizationMeta input ) {
        return "Input 2".equals( input.getStepName() );
      }
    } );
    assertThat( input2Opt.getName(), containsString( "Input 2" ) );
    assertThat( input2Opt.getType(), instanceOf( ParameterGeneration.class ) );
    List<SourceTargetFields> input2Mappings = ( (ParameterGeneration) input2Opt.getType() ).getFieldMappings();
    assertThat( Iterables.getOnlyElement( input2Mappings ), allOf(
      isA( SourceTargetFields.class ),
      hasProperty( "sourceFieldName", equalTo( "field1" ) ),
      hasProperty( "targetFieldName", equalTo( "field1" ) )
    ) );
  }
}
