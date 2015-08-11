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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import org.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptDialog;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.serialization.DataServiceMetaStoreUtil;
import org.pentaho.di.trans.dataservice.ui.DataServiceDelegate;
import org.pentaho.di.trans.dataservice.ui.DataServiceDialog;
import org.pentaho.di.trans.dataservice.ui.DataServiceDialogCallback;
import org.pentaho.di.trans.dataservice.ui.DataServiceTestDialog;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
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
                                      TransMeta transMeta, DataServiceDelegate delegate )
    throws KettleException {
    setName( NAME );
    this.delegate = delegate;
    this.parent = parent;
    this.model = model;
    this.transMeta = transMeta;
    this.spoon = this.delegate.getSpoon();
    this.dataService = dataService;
    metaStoreUtil = this.delegate.getMetaStoreUtil();
    pushDownFactories = this.delegate.getPushDownFactories();
    autoOptimizationServices = this.delegate.getAutoOptimizationServices();
  }

  public void init() throws InvocationTargetException, XulException, KettleException {
    bindingFactory = new DefaultBindingFactory();
    bindingFactory.setDocument( this.getXulDomContainer().getDocumentRoot() );
    createBindings();
    setModel();
  }

  private void createBindings() {
    XulTextbox transName = (XulTextbox) document.getElementById( "trans-name" );
    XulMenuList steps = (XulMenuList) document.getElementById( "trans-steps" );
    optimizations = (XulTree) document.getElementById( "optimizations" );
    XulTextbox serviceName = (XulTextbox) document.getElementById( "service-name" );

    bindingFactory.setBindingType( Binding.Type.ONE_WAY );
    bindingFactory.createBinding( model, "steps", steps, "elements" );
    optimizationBinding = bindingFactory.createBinding( model, "pushDownOptimizations", optimizations, "elements" );
    stepBinding = bindingFactory.createBinding( model, "selectedStep", steps, "selectedItem" );
    bindingFactory.createBinding( steps, "selectedItem", model, "selectedStep" );
    bindingFactory.createBinding( model, "transName", transName, "value" );

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
    if ( !validate() ) {
      return;
    }

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
    if ( dataService.getName() != null && ( !dataService.getName().equals( model.getServiceName() ) || !dataService
      .getStepname()
      .equals( model.getSelectedStep() ) ) ) {
      removeDataService();
    }
    try {
      metaStoreUtil.save( spoon.getRepository(), spoon.getMetaStore(), getDataService() );
      spoon.refreshTree();
      spoon.refreshGraph();
    } catch ( MetaStoreException e ) {
      throw new KettleException( BaseMessages.getString( PKG, "DataServiceDialog.MetaStore.Error" ) );
    }
  }

  public void saveAndClose() throws KettleException, XulException, InvocationTargetException {
    if ( !validate() ) {
      return;
    }

    save();
    close();
  }

  public void addOptimization() {
    PushDownOptimizationMeta optMeta = new PushDownOptimizationMeta();
    PushDownOptDialog dialog =
      new PushDownOptDialog( (Shell) parent, PropsUI.getInstance(), transMeta, optMeta, pushDownFactories );
    if ( dialog.open() == SWT.OK ) {
      List<PushDownOptimizationMeta> optimizations = model.getPushDownOptimizations();
      optMeta.getType().init( transMeta, getDataService(), optMeta );
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

  private void removeDataService() {
    delegate.removeDataService( transMeta, dataService, false );
  }

  private DataServiceMeta getDataService() {
    DataServiceMeta dataService = new DataServiceMeta( transMeta );
    dataService.setName( model.getServiceName() );
    dataService.setPushDownOptimizationMeta( model.getPushDownOptimizations() );
    dataService.setStepname( model.getSelectedStep() );

    return dataService;
  }

  public Boolean validate() throws KettleException {
    List<String> errors = Lists.newArrayList();

    if ( Const.isEmpty( model.getServiceName() ) ) {
      errors.add( BaseMessages.getString( PKG, "DataServiceDialog.NameRequired.Error" ) );
    }

    if ( Const.isEmpty( model.getSelectedStep() ) ) {
      errors.add( BaseMessages.getString( PKG, "DataServiceDialog.StepRequired.Error" ) );
    }

    if ( !Const.isEmpty( model.getServiceName() ) &&
      ( dataService.getName() == null || !dataService.getName().equals( model.getServiceName() ) ) ) {
      try {
        if ( delegate.getDataServiceNames().contains( model.getServiceName() ) ) {
          String msg = BaseMessages.getString( PKG, "DataServiceDialog.AlreadyExists.Error", model.getServiceName() );
          errors.add( msg );
        }
      } catch ( MetaStoreException e ) {
        // Ignore this error case
      }
    }

    if ( errors.size() > 0 ) {
      delegate.showErrors(
        BaseMessages.getString( PKG, "DataServiceDialog.Errors.Title" ),
        Joiner.on( '\n' ).join( errors )
      );
      return false;
    }

    return true;
  }

  public void close() {
    callback.onClose();
  }

  public void setCallback( DataServiceDialogCallback callback ) {
    this.callback = callback;
  }
}
