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

package com.pentaho.di.trans.dataservice.ui.controller;

import com.pentaho.di.trans.dataservice.DataServiceMeta;
import com.pentaho.di.trans.dataservice.DataServiceMetaStoreUtil;
import com.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import com.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import com.pentaho.di.trans.dataservice.optimization.paramgen.AutoParameterGenerationService;
import com.pentaho.di.trans.dataservice.ui.DataServiceDialogCallback;
import com.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Shell;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;
import static junit.framework.Assert.*;

public class DataServiceDialogControllerTest {

  private DataServiceDialogController controller;

  private Composite parent;

  private DataServiceModel model;

  private DataServiceMeta dataService;

  private TransMeta transMeta;

  private List<AutoOptimizationService> autoOptimizationServices;

  private List<PushDownFactory> pushDownFactories;

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
    parent = mock( Shell.class );
    model = mock( DataServiceModel.class );
    dataService = mock( DataServiceMeta.class );
    transMeta = mock( TransMeta.class );
    spoon = mock( Spoon.class );
    metaStore = mock( DelegatingMetaStore.class );
    sharedObjects = mock( SharedObjects.class );
    autoParameterGenerationService = mock( AutoParameterGenerationService.class );
    callback = mock( DataServiceDialogCallback.class );
    metaStoreUtil = mock( DataServiceMetaStoreUtil.class );

    autoOptimizationServices = new ArrayList<AutoOptimizationService>();
    autoOptimizationServices.add( autoParameterGenerationService );

    pushDownFactories = new ArrayList<PushDownFactory>();

    controller = new DataServiceDialogControllerTester( parent, model, dataService, metaStoreUtil, transMeta, spoon,
      autoOptimizationServices, pushDownFactories );
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
    public DataServiceDialogControllerTester( Composite parent, DataServiceModel model, DataServiceMeta dataService,
                                              DataServiceMetaStoreUtil metaStoreUtil, TransMeta transMeta, Spoon spoon,
                                              List<AutoOptimizationService> autoOptimizationServices,
                                              List<PushDownFactory> pushDownFactories )
      throws KettleException {
      super( parent, model, dataService, metaStoreUtil, transMeta, spoon, autoOptimizationServices, pushDownFactories );
    }

    @Override
    protected void showErrors( StringBuilder errors ) {
      // Show nothing
    }
  }
}
