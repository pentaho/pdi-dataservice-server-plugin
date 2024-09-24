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

package org.pentaho.di.trans.dataservice.ui.model;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
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
import static org.junit.Assert.assertEquals;
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
@RunWith( MockitoJUnitRunner.StrictStubs.class)
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

    assertFalse( model.remove( optimizationMeta ) );
    verifyNoMoreInteractions( propertyChangeSupport );

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
  public void testGetDataServiceServiceLimits() {
    int MAX_SERVICE_ROWS = 100;
    long MAX_SERVICE_TIME = 200;

    model.setServiceName( "service" );
    model.setServiceStep( "step" );
    model.setServiceMaxRows( MAX_SERVICE_ROWS );
    model.setServiceMaxTime( MAX_SERVICE_TIME );

    DataServiceMeta dataService = model.getDataService();
    assertThat( dataService.getRowLimit(), equalTo( MAX_SERVICE_ROWS ) );
    assertThat( dataService.getTimeLimit(), equalTo( MAX_SERVICE_TIME ) );

    model.setServiceMaxRows( 0 );
    model.setServiceMaxTime( 0 );

    dataService = model.getDataService();
    assertThat( dataService.getRowLimit(), equalTo( 0 ) );
    assertThat( dataService.getTimeLimit(), equalTo( 0L ) );
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
  public void testGetStepFieldsNullTransMeta() {
    model = new DataServiceModel( null );
    model.setServiceStep( "step" );

    ImmutableList<String> stepFields = model.getStepFields();

    assertThat( stepFields, equalTo( ImmutableList.of() ) );
  }

  @Test
  public void testGetStepFieldsNullServiceStep() {
    model.setServiceStep( null );

    ImmutableList<String> stepFields = model.getStepFields();

    assertThat( stepFields, equalTo( ImmutableList.of() ) );
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
  public void testStreaming() {
    model.setStreaming( false );
    assertFalse( model.isStreaming() );
    model.setStreaming( true );
    assertTrue( model.isStreaming() );
  }

  @Test
  public void testServiceMaxRows() {
    int MAX_ROWS = 9999;
    assertEquals( 0,  model.getServiceMaxRows() );
    model.setServiceMaxRows( MAX_ROWS );
    assertEquals( MAX_ROWS, model.getServiceMaxRows() );
  }

  @Test
  public void testServiceMaxTime() {
    long MAX_TIME = 9999;
    assertEquals( 0, model.getServiceMaxTime() );
    model.setServiceMaxTime( MAX_TIME );
    assertEquals( MAX_TIME, model.getServiceMaxTime() );
  }
}
