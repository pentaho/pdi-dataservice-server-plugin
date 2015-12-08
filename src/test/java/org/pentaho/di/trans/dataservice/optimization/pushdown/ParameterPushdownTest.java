/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.optimization.pushdown;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

/**
 * @author nhudak
 */

@RunWith( org.mockito.runners.MockitoJUnitRunner.class )
public class ParameterPushdownTest {
  @Mock ParameterPushdownFactory factory;
  @Mock TransMeta transMeta;

  @InjectMocks ParameterPushdown parameterPushdown;
  private PushDownOptimizationMeta optimizationMeta;
  private DataServiceMeta dataService;

  @Before
  public void setUp() throws Exception {
    optimizationMeta = new PushDownOptimizationMeta();
    optimizationMeta.setType( parameterPushdown );

    dataService = new DataServiceMeta( transMeta );
    dataService.setStepname( "OUTPUT" );
    dataService.getPushDownOptimizationMeta().add( optimizationMeta );
  }

  @Test
  public void testInit() throws Exception {
    ParameterPushdown.Definition definition;
    parameterPushdown.createDefinition();

    definition = parameterPushdown.createDefinition();
    definition.setFieldName( "field only" );

    definition = parameterPushdown.createDefinition();
    definition.setParameter( "param only" );

    definition = parameterPushdown.createDefinition();
    definition.setFieldName( "both field" );
    definition.setParameter( "and param" );
    assertThat( definition.getFormat(), is( ParameterPushdown.DEFAULT_FORMAT ) );
    definition.setFormat( "prefix: %s" );
    assertThat( definition.getFormat(), is( "prefix: %s" ) );

    assertThat( parameterPushdown.getDefinitions(), hasSize( 4 ) );

    parameterPushdown.init( transMeta, dataService, optimizationMeta );

    assertThat( optimizationMeta.getStepName(), is( "OUTPUT" ) );

    // Only the last should remain
    assertThat( parameterPushdown.getDefinitions(), contains( definition ) );
    verify( transMeta ).addParameterDefinition( eq( "and param" ), argThat( emptyString() ), anyString() );
    verify( transMeta ).activateParameters();
  }
}
