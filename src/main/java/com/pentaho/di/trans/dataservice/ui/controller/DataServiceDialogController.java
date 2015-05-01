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

import com.pentaho.di.trans.dataservice.DataServiceDelegate;
import com.pentaho.di.trans.dataservice.DataServiceMeta;
import com.pentaho.di.trans.dataservice.DataServiceMetaStoreUtil;
import com.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import com.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptDialog;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import com.pentaho.di.trans.dataservice.ui.DataServiceDialog;
import com.pentaho.di.trans.dataservice.ui.DataServiceDialogCallback;
import com.pentaho.di.trans.dataservice.ui.DataServiceTestDialog;
import com.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.binding.DefaultBindingFactory;
import org.pentaho.ui.xul.components.XulMenuList;
import org.pentaho.ui.xul.components.XulTextbox;
import org.pentaho.ui.xul.containers.XulTree;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

public class DataServiceDialogController extends AbstractXulEventHandler {

  private List<AutoOptimizationService> autoOptimizationServices;

  private DataServiceDelegate delegate;
  private DataServiceMeta dataService;
  private TransMeta transMeta;
  private DataServiceDialogCallback callback;
  private DataServiceModel model;
  private BindingFactory bindingFactory;
  private Composite parent;
  private Binding stepBinding;
  private Binding optimizationBinding;
  private XulTree optimizations;
  private Spoon spoon;
  private DataServiceMetaStoreUtil metaStoreUtil;
  private List<PushDownFactory> pushDownFactories;

  private static final Class<?> PKG = DataServiceDialog.class;

  private static final String NAME = "dataServiceDialogController";

  public DataServiceDialogController( Composite parent, DataServiceModel model, DataServiceMeta dataService,
                                      DataServiceMetaStoreUtil metaStoreUtil, TransMeta transMeta, Spoon spoon,
                                      List<AutoOptimizationService> autoOptimizationServices,
                                      List<PushDownFactory> pushDownFactories )
    throws KettleException {
    setName( NAME );
    this.delegate = new DataServiceDelegate( metaStoreUtil, autoOptimizationServices, pushDownFactories );
    this.autoOptimizationServices = autoOptimizationServices;
    this.parent = parent;
    this.model = model;
    this.transMeta = transMeta;
    this.spoon = spoon;
    this.dataService = dataService;
    this.metaStoreUtil = metaStoreUtil;
    this.pushDownFactories = pushDownFactories;
  }

  public void init() throws InvocationTargetException, XulException, KettleException {
    bindingFactory = new DefaultBindingFactory();
    bindingFactory.setDocument( this.getXulDomContainer().getDocumentRoot() );
    createBindings();
    setModel();
  }

  private void createBindings() {
    XulTextbox transName = (XulTextbox) document.getElementById( "trans-name" );
    XulTextbox transLocation = (XulTextbox) document.getElementById( "trans-location" );
    XulMenuList steps = (XulMenuList) document.getElementById( "trans-steps" );
    optimizations = (XulTree) document.getElementById( "optimizations" );
    XulTextbox serviceName = (XulTextbox) document.getElementById( "service-name" );

    bindingFactory.setBindingType( Binding.Type.ONE_WAY );
    bindingFactory.createBinding( model, "steps", steps, "elements" );
    optimizationBinding = bindingFactory.createBinding( model, "pushDownOptimizations", optimizations, "elements" );
    stepBinding = bindingFactory.createBinding( model, "selectedStep", steps, "selectedItem" );
    bindingFactory.createBinding( steps, "selectedItem", model, "selectedStep" );
    bindingFactory.createBinding( model, "transName", transName, "value" );
    bindingFactory.createBinding( model, "transLocation", transLocation, "value" );

    bindingFactory.setBindingType( Binding.Type.BI_DIRECTIONAL );
    bindingFactory.createBinding( model, "serviceName", serviceName, "value" );
  }

  private void setModel() throws XulException, InvocationTargetException {
    model.setPushDownOptimizations( dataService.getPushDownOptimizationMeta() );
    model.setServiceName( dataService.getName() );
    model.setTransName( transMeta.getName() );

    if ( transMeta.getRepository() != null ) {
      model.setTransLocation( transMeta.getRepositoryDirectory().getPath() );
    } else {
      model.setTransLocation( transMeta.getFilename() );
    }

    model.setSteps( Arrays.asList( transMeta.getStepNames() ) );
    if ( dataService.getStepname() != null ) {
      model.setSelectedStep( dataService.getStepname() );
    }

    stepBinding.fireSourceChanged();
  }

  public void showTestDialog() throws KettleException {
    validate();

    new DataServiceTestDialog( parent, getDataService(), this.transMeta ).open();
  }

  public void getOptimizations() throws XulException, InvocationTargetException {
    transMeta.setMetaStore( spoon.getMetaStore() );
    spoon.sharedObjectsFileMap.put( transMeta.getSharedObjects().getFilename(), transMeta.getSharedObjects() );
    spoon.setTransMetaVariables( transMeta );
    transMeta.clearChanged();
    transMeta.activateParameters();
    for ( AutoOptimizationService autoOptimizationService : autoOptimizationServices ) {
      model.addPushDownOptimizations( autoOptimizationService.apply( transMeta, getDataService() ) );
    }
  }

  public void viewStep() {
    StepMeta viewStep = null;
    for ( StepMeta step : transMeta.getSteps() ) {
      if ( step.getName().equals( model.getSelectedStep() ) ) {
        viewStep = step;
      }
    }
    if ( viewStep != null ) {
      callback.onHideStep();
      spoon.editStep( transMeta, viewStep );
      callback.onViewStep();
    }
  }

  public void save() throws KettleException {
    validate();

    if ( dataService.getName() != null && ( !dataService.getName().equals( model.getServiceName() ) || !dataService
      .getStepname()
      .equals( model.getSelectedStep() ) ) ) {
      removeDataService();
    }
    try {
      metaStoreUtil.toTransMeta( transMeta, spoon.getMetaStore(), getDataService() );
      spoon.refreshTree();
    } catch ( MetaStoreException e ) {
      throw new KettleException( BaseMessages.getString( PKG, "DataServiceDialog.MetaStore.Error" ) );
    }
  }

  public void saveAndClose() throws KettleException, XulException, InvocationTargetException {
    save();
    close();
  }

  public void addOptimization() {
    PushDownOptimizationMeta optMeta = new PushDownOptimizationMeta();
    PushDownOptDialog dialog =
      new PushDownOptDialog( (Shell) parent, PropsUI.getInstance(), transMeta, optMeta, pushDownFactories );
    if ( dialog.open() == SWT.OK ) {
      List<PushDownOptimizationMeta> optimizations = model.getPushDownOptimizations();
      optimizations.add( optMeta );
      model.setPushDownOptimizations( optimizations );
    }
  }

  public void editOptimization() throws XulException, InvocationTargetException {
    if ( optimizations.getSelectedItem() != null ) {
      PushDownOptDialog dialog = new PushDownOptDialog( (Shell) parent, PropsUI.getInstance(), transMeta,
        (PushDownOptimizationMeta) optimizations.getSelectedItem(), pushDownFactories );
      if ( dialog.open() == SWT.OK ) {
        optimizationBinding.fireSourceChanged();
      }
    }
  }

  public void removeOptimization() throws XulException, InvocationTargetException {
    if ( optimizations.getSelectedItem() != null ) {
      model.removePushDownOptimization( (PushDownOptimizationMeta) optimizations.getSelectedItem() );
    }
  }

  public void openTrans() throws KettleException {
    delegate.openTrans( transMeta );
  }

  private void removeDataService() {
    delegate.removeDataService( dataService, false );
  }

  private DataServiceMeta getDataService() {
    DataServiceMeta dataService = new DataServiceMeta();
    dataService.setName( model.getServiceName() );
    dataService.setPushDownOptimizationMeta( model.getPushDownOptimizations() );
    dataService.setStepname( model.getSelectedStep() );

    return dataService;
  }

  public void validate() throws KettleException {
    if ( Const.isEmpty( model.getServiceName() ) ) {
      throw new KettleException( BaseMessages.getString( PKG, "DataServiceDialog.NameRequired.Error" ) );
    }

    if ( Const.isEmpty( model.getSelectedStep() ) ) {
      throw new KettleException( BaseMessages.getString( PKG, "DataServiceDialog.StepRequired.Error" ) );
    }
  }

  public void close() {
    callback.onClose();
  }

  public void setCallback( DataServiceDialogCallback callback ) {
    this.callback = callback;
  }
}
