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

package com.pentaho.di.trans.dataservice;

import com.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import com.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import com.pentaho.di.trans.dataservice.ui.DataServiceTestDialog;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

import java.util.List;

public class DataServiceStepHandler extends AbstractXulEventHandler {

  public static Class<?> PKG = DataServiceStepHandler.class;

  private final String HANDLER_NAME = "dataServiceStepHandler";

  private DataServiceMetaStoreUtil metaStoreUtil;
  private DataServiceDelegate delegate;
  private List<PushDownFactory> pushDownFactories;

  public DataServiceStepHandler( DataServiceMetaStoreUtil metaStoreUtil,
                                 List<AutoOptimizationService> autoOptimizationServices,
                                 List<PushDownFactory> pushDownFactories ) {
    this.metaStoreUtil = metaStoreUtil;
    delegate = new DataServiceDelegate( metaStoreUtil, autoOptimizationServices, pushDownFactories );
  }

  @Override
  public String getName() {
    return HANDLER_NAME;
  }

  public void newDataService() throws KettleException {
    delegate.createNewDataService( getCurrentStep().getName() );
  }

  public void editDataService() throws KettleException, MetaStoreException {
    DataServiceMeta dataService =
      metaStoreUtil.fromTransMeta( getActiveTrans(), getMetaStore(), getCurrentStep().getName() );

    delegate.editDataService( dataService );
  }

  public void deleteDataService() throws KettleException, MetaStoreException {
    DataServiceMeta dataService =
      metaStoreUtil.fromTransMeta( getActiveTrans(), getMetaStore(), getCurrentStep().getName() );

    delegate.removeDataService( dataService );
  }

  public void testDataService() throws MetaStoreException, KettleException {
    DataServiceMeta dataService =
      metaStoreUtil.fromTransMeta( getActiveTrans(), getMetaStore(), getCurrentStep().getName() );
    new DataServiceTestDialog( getShell(), dataService, getActiveTrans() ).open();
  }

  private Spoon getSpoon() {
    return Spoon.getInstance();
  }

  private Shell getShell() {
    return getSpoon().getShell();
  }

  private StepMeta getCurrentStep() {
    return getSpoon().getActiveTransGraph().getCurrentStep();
  }

  private IMetaStore getMetaStore() {
    return getSpoon().getMetaStore();
  }

  private TransMeta getActiveTrans() {
    return getSpoon().getActiveTransformation();
  }
}
