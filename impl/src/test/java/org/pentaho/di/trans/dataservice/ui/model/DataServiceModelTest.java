/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.ui.model;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownType;
import org.pentaho.di.trans.step.StepMeta;

import java.beans.PropertyChangeSupport;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.class )
public class DataServiceModelTest {

  @Mock TransMeta transMeta;
  @Mock PropertyChangeSupport propertyChangeSupport;
  @Mock StepMeta stepMeta;
  @Mock RowMetaInterface rowMetaInterface;
  @Mock ValueMetaInterface valueMetaInterface;
  DataServiceModel model;

  @Before
  public void setUp() throws Exception {
    model = new DataServiceModel( transMeta ) {
      {
        changeSupport = propertyChangeSupport;
      }
    };
  }

  @Test
  public void testGetTransMeta() throws Exception {
    assertThat( model.getTransMeta(), sameInstance( transMeta ) );
  }

  @Test
  public void testServiceName() throws Exception {
    model.setServiceName( "service" );
    verify( propertyChangeSupport ).firePropertyChange( "serviceName", null, "service" );
    model.setServiceName( "new-service" );
    verify( propertyChangeSupport ).firePropertyChange( "serviceName", "service", "new-service" );
    assertThat( model.getServiceName(), equalTo( "new-service" ) );
  }

  @Test
  public void testServiceStep() throws Exception {
    model.setServiceStep( "service" );
    verify( propertyChangeSupport ).firePropertyChange( "serviceStep", null, "service" );
    model.setServiceStep( "new-service" );
    verify( propertyChangeSupport ).firePropertyChange( "serviceStep", "service", "new-service" );
    assertThat( model.getServiceStep(), equalTo( "new-service" ) );
  }

  @Test
  public void testPushDownOptimizations() throws Exception {
    List<PushDownOptimizationMeta> emptyList = Collections.emptyList();
    List<PushDownOptimizationMeta> optimizations = Lists.newArrayList( mock( PushDownOptimizationMeta.class ) );

    model.setPushDownOptimizations( optimizations );
    verify( propertyChangeSupport ).firePropertyChange( "pushDownOptimizations", emptyList, optimizations );
    ImmutableList<PushDownOptimizationMeta> init = ImmutableList.copyOf( optimizations );

    PushDownOptimizationMeta optimizationMeta = mock( PushDownOptimizationMeta.class );
    optimizations.add( optimizationMeta );

    assertTrue( model.add( optimizationMeta ) );
    verify( propertyChangeSupport ).firePropertyChange( "pushDownOptimizations", init, optimizations );

    PushDownType pushDownType = mock( PushDownType.class );
    when( optimizationMeta.getType() ).thenReturn( pushDownType );
    assertThat( model.getPushDownOptimizations( pushDownType.getClass() ), contains( optimizationMeta ) );
    assertThat( model.getPushDownOptimizations(), equalTo( optimizations ) );

    assertTrue( model.remove( optimizationMeta ) );
    verify( propertyChangeSupport ).firePropertyChange( "pushDownOptimizations", optimizations, init );

    assertTrue( model.removeAll( optimizations ) );
    verify( propertyChangeSupport ).firePropertyChange( "pushDownOptimizations", init, emptyList );

    assertFalse( model.removeAll( optimizations ) );
    verifyNoMoreInteractions( propertyChangeSupport );
  }

  @Test
  public void testGetDataService() throws Exception {
    model.setServiceName( "service" );
    model.setServiceStep( "step" );
    PushDownOptimizationMeta optimizationMeta = mock( PushDownOptimizationMeta.class );
    model.setPushDownOptimizations( Lists.newArrayList( optimizationMeta ) );

    PushDownType pushDownType = mock( PushDownType.class );
    when( optimizationMeta.getType() ).thenReturn( pushDownType );

    DataServiceMeta dataService = model.getDataService();
    assertThat( dataService, allOf(
      hasProperty( "name", equalTo( "service" ) ),
      hasProperty( "stepname", equalTo( "step" ) ),
      hasProperty( "pushDownOptimizationMeta", contains( optimizationMeta ) )
    ) );
    verify( pushDownType ).init( transMeta, dataService, optimizationMeta );
  }

  @Test
  public void testGetStepFields() throws Exception {
    model.setServiceStep( "step" );
    when( transMeta.findStep( model.getServiceStep() ) ).thenReturn( stepMeta );
    when( transMeta.getStepFields( stepMeta ) ).thenReturn( rowMetaInterface );
    when( rowMetaInterface.size() ).thenReturn( 1 );
    when( rowMetaInterface.getValueMeta( 0 ) ).thenReturn( valueMetaInterface );
    when( valueMetaInterface.getName() ).thenReturn( "serviceField" );

    ImmutableList<String> stepFields = model.getStepFields();

    verify( transMeta ).findStep( "step" );
    verify( transMeta ).getStepFields( stepMeta );

    assertThat( stepFields.size(), equalTo( 1 ) );
    assertThat( stepFields.get( 0 ), equalTo( "serviceField" ) );
  }

  @Test
  public void testGetStepFieldsException() throws Exception {
    model.setServiceStep( "step" );
    when( transMeta.findStep( model.getServiceStep() ) ).thenReturn( stepMeta );
    when( transMeta.getStepFields( stepMeta ) ).thenThrow( new KettleStepException() );

    ImmutableList<String> stepFields = model.getStepFields();

    verify( transMeta ).findStep( "step" );
    verify( transMeta ).getStepFields( stepMeta );

    assertThat( stepFields.size(), equalTo( 0 ) );
  }

  @Test
  public void testStreaming() throws Exception {
    model.setStreaming( false );
    assertFalse( model.isStreaming() );
    model.setStreaming( true );
    assertTrue( model.isStreaming() );
  }
}
