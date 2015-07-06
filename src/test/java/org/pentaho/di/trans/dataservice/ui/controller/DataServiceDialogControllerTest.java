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

package org.pentaho.di.trans.dataservice.ui.controller;

import com.google.common.collect.ImmutableList;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Shell;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.DataServiceMetaStoreUtil;
import org.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.paramgen.AutoParameterGenerationService;
import org.pentaho.di.trans.dataservice.ui.DataServiceDialogCallback;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataServiceDialogControllerTest {

  private DataServiceDialogController controller;

  private DataServiceModel model;

  private DataServiceMeta dataService;

  private TransMeta transMeta;

  private DataServiceMetaStoreUtil metaStoreUtil;

  private Spoon spoon;

  private IMetaStore metaStore;

  private SharedObjects sharedObjects;

  private AutoParameterGenerationService autoParameterGenerationService;

  private DataServiceDialogCallback callback;

  private static final String FILE_NAME = "/home/admin/transformation.ktr";

  private static final String SERVICE_NAME = "test_service";

  private static final String NEW_SERVICE_NAME = "test_service2";

  private static final String SELECTED_STEP = "Output Step";

  private static final String STEP_ONE_NAME = "Step One";

  private static final String STEP_TWO_NAME = "Step Two";

  @Before
  public void init() throws Exception {
    Composite parent = mock( Shell.class );
    model = mock( DataServiceModel.class );
    dataService = mock( DataServiceMeta.class );
    transMeta = mock( TransMeta.class );
    spoon = mock( Spoon.class );
    metaStore = mock( DelegatingMetaStore.class );
    sharedObjects = mock( SharedObjects.class );
    autoParameterGenerationService = mock( AutoParameterGenerationService.class );
    callback = mock( DataServiceDialogCallback.class );
    metaStoreUtil = mock( DataServiceMetaStoreUtil.class );

    DataServiceContext context = mock( DataServiceContext.class );
    when( context.getMetaStoreUtil() ).thenReturn( metaStoreUtil );
    when( context.getAutoOptimizationServices() ).thenReturn(
      ImmutableList.<AutoOptimizationService>of( autoParameterGenerationService ) );
    when( context.getPushDownFactories() ).thenReturn( new ArrayList<PushDownFactory>() );

    controller = new DataServiceDialogControllerTester( parent, model, dataService, transMeta, spoon, context );
    controller.setCallback( callback );

    doReturn( SERVICE_NAME ).when( dataService ).getName();
  }

  @Test
  public void testGetOptimizations() throws Exception {
    spoon.sharedObjectsFileMap = mock( Map.class );

    List<PushDownOptimizationMeta> pushDownOptimizations = new ArrayList<PushDownOptimizationMeta>();

    doReturn( metaStore ).when( spoon ).getMetaStore();
    doReturn( sharedObjects ).when( transMeta ).getSharedObjects();
    doReturn( FILE_NAME ).when( sharedObjects ).getFilename();
    doReturn( pushDownOptimizations ).when( model ).getPushDownOptimizations();

    controller.getOptimizations();

    verify( transMeta ).setMetaStore( metaStore );
    verify( spoon.sharedObjectsFileMap ).put( sharedObjects.getFilename(), sharedObjects );
    verify( spoon ).setTransMetaVariables( transMeta );
    verify( transMeta ).clearChanged();
    verify( transMeta ).activateParameters();
    verify( model ).addPushDownOptimizations( pushDownOptimizations );
    verify( autoParameterGenerationService ).apply( any( TransMeta.class ), any( DataServiceMeta.class ) );
  }

  @Test
  public void viewStep() throws Exception {
    StepMeta step1 = mock( StepMeta.class );
    doReturn( STEP_ONE_NAME ).when( step1 ).getName();

    StepMeta step2 = mock( StepMeta.class );
    doReturn( STEP_TWO_NAME ).when( step2 ).getName();

    List<StepMeta> steps = new ArrayList<StepMeta>();
    steps.add( step1 );
    steps.add( step2 );

    doReturn( steps ).when( transMeta ).getSteps();
    doReturn( STEP_ONE_NAME ).when( model ).getSelectedStep();

    controller.viewStep();

    verify( transMeta ).getSteps();
    verify( step1 ).getName();
    verify( step2 ).getName();
    verify( model, times( 2 ) ).getSelectedStep();
    verify( callback ).onViewStep();
    verify( spoon ).editStep( transMeta, step1 );
    verify( callback ).onHideStep();
  }

  @Test
  public void testValidate() throws Exception {
    doReturn( SERVICE_NAME ).when( model ).getServiceName();
    doReturn( SELECTED_STEP ).when( model ).getSelectedStep();

    controller.validate();

    verify( dataService, times( 2 ) ).getName();
    verify( model, times( 3 ) ).getServiceName();
    verify( model ).getSelectedStep();
  }

  @Test
  public void testError() throws Exception {
    doReturn( "" ).when( model ).getServiceName();

    if ( controller.validate() ) {
      fail();
    }

    doReturn( SERVICE_NAME ).when( model ).getServiceName();
    doReturn( "" ).when( model ).getSelectedStep();

    if ( controller.validate() ) {
      fail();
    }

    IMetaStore metaStore = mock( DelegatingMetaStore.class );
    doReturn( metaStore ).when( spoon ).getMetaStore();
    doReturn( SELECTED_STEP ).when( model ).getSelectedStep();
    doReturn( SERVICE_NAME ).when( model ).getServiceName();
    doReturn( NEW_SERVICE_NAME ).when( dataService ).getName();
    doReturn( mock( DataServiceMeta.class ) ).when( metaStoreUtil ).findByName( metaStore, SERVICE_NAME );

    if ( controller.validate() ) {
      fail();
    }

    verify( model, times( 10 ) ).getServiceName();
    verify( model, times( 3 ) ).getSelectedStep();
    verify( dataService, times( 4 ) ).getName();
    verify( metaStoreUtil ).findByName( metaStore, SERVICE_NAME );
  }

  class DataServiceDialogControllerTester extends DataServiceDialogController {
    public DataServiceDialogControllerTester( Composite parent, DataServiceModel model,
                                              DataServiceMeta dataService, TransMeta transMeta,
                                              Spoon spoon, DataServiceContext context )
      throws KettleException {
      super( parent, model, dataService, transMeta, spoon, context );
    }

    @Override
    protected void showErrors( StringBuilder errors ) {
      // Show nothing
    }
  }
}
