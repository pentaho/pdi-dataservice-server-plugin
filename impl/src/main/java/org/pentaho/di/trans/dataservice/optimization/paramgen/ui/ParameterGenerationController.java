/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.optimization.paramgen.ui;

import com.google.common.base.Strings;
import org.eclipse.swt.SWT;
import org.pentaho.di.trans.dataservice.optimization.AutoOptimizationService;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGeneration;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGenerationFactory;
import org.pentaho.di.trans.dataservice.ui.controller.AbstractController;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.components.XulMenuList;
import org.pentaho.ui.xul.components.XulMessageBox;
import org.pentaho.ui.xul.components.XulPromptBox;
import org.pentaho.ui.xul.util.XulDialogCallback;

import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.pentaho.di.i18n.BaseMessages.getString;
import static org.pentaho.di.trans.dataservice.ui.BindingConverters.keySet;
import static org.pentaho.di.trans.dataservice.ui.BindingConverters.not;
import static org.pentaho.di.trans.dataservice.ui.BindingConverters.stringIsEmpty;

/**
 * @author nhudak
 */
public class ParameterGenerationController extends AbstractController {
  private static final Class<?> PKG = ParameterGenerationController.class;
  private static final String NAME = "paramGenCtrl";

  {
    setName( NAME );
  }

  private final ParameterGenerationFactory factory;
  private final ParameterGenerationModel model;

  public ParameterGenerationController( ParameterGenerationFactory factory, ParameterGenerationModel model ) {
    this.factory = factory;
    this.model = model;
  }

  public void initBindings( List<String> supportedSteps ) {
    BindingFactory bindingFactory = getBindingFactory();

    XulMenuList<String> stepList = getStepMenuList();
    stepList.setElements( supportedSteps );
    getElementById( "param_gen_add" ).setDisabled( supportedSteps.isEmpty() );

    // BI DIRECTIONAL bindings
    bindingFactory.setBindingType( Binding.Type.BI_DIRECTIONAL );
    bindingFactory.createBinding( model, "selectedParameter", "param_gen_list", "selectedItem" );
    bindingFactory.createBinding( model, "selectedStep", stepList, "value" );
    bindingFactory.createBinding( model, "enabled", "param_gen_enabled", "checked", not() );

    // ONE WAY bindings
    bindingFactory.setBindingType( Binding.Type.ONE_WAY );
    bindingFactory.createBinding( model, "parameterMap", "param_gen_list", "elements", keySet() );
    bindingFactory.createBinding( model, "selectedParameter", "param_gen_edit", "disabled", stringIsEmpty() );
    bindingFactory.createBinding( model, "selectedParameter", "param_gen_remove", "disabled", stringIsEmpty() );
    bindingFactory.createBinding( model, "selectedParameter", "param_gen_enabled", "disabled", stringIsEmpty() );
    bindingFactory.createBinding( model, "enabled", "param_gen_step", "disabled", not() );
    bindingFactory.createBinding( model, "enabled", "param_gen_mapping", "disabled", not() );
    bindingFactory.createBinding( model, "mappings", "param_gen_mapping", "elements" );

    model.updateParameterMap();
  }

  private XulMenuList<String> getStepMenuList() {
    return getElementById( "param_gen_step" );
  }

  public void runAutoGenerate() throws XulException {
    DataServiceModel dialogModel = model.getDialogModel();
    try {
      AutoOptimizationService autoOptimizationService = factory.createAutoOptimizationService();
      Collection<PushDownOptimizationMeta> found = autoOptimizationService.apply( dialogModel.getDataService() );

      if ( dialogModel.addAll( found ) ) {
        model.updateParameterMap();
      }

      info( getString( PKG, "ParameterGenerationController.AutoGen.Title" ),
        getString( PKG, "ParameterGenerationController.AutoGen.Message", found.size() ) );
    } catch ( Exception e ) {
      String message = getString( PKG, "ParameterGenerationController.AutoGen.Error" );
      getLogChannel().logError( message, e );

      error( getString( PKG, "ParameterGenerationController.AutoGen.Title" ), message );
    }
  }

  public void addParameter() throws XulException {
    PushDownOptimizationMeta meta = new PushDownOptimizationMeta();
    ParameterGeneration parameterGeneration = factory.createPushDown();
    meta.setType( parameterGeneration );
    meta.setStepName( getStepMenuList().getSelectedItem() );

    XulPromptBox promptBox = createPromptBox();
    promptBox.setTitle( getString( PKG, "ParameterGenerationController.Create.Title" ) );
    promptBox.setMessage( getString( PKG, "ParameterGenerationController.Create.Message" ) );
    ParameterEditor editor = new ParameterEditor( parameterGeneration );
    promptBox.addDialogCallback( editor );

    if ( promptBox.open() == 0 && editor.modified ) {
      model.add( meta );
      model.setSelectedParameter( parameterGeneration.getParameterName() );
    }
  }

  public void editParameter() throws XulException {
    ParameterGeneration parameterGeneration = checkNotNull( model.getParameterGeneration() );

    XulPromptBox promptBox = createPromptBox();
    promptBox.setTitle( getString( PKG, "ParameterGenerationController.Edit.Title" ) );
    promptBox.setMessage( getString( PKG, "ParameterGenerationController.Edit.Message" ) );
    promptBox.setValue( parameterGeneration.getParameterName() );

    ParameterEditor editor = new ParameterEditor( parameterGeneration );
    promptBox.addDialogCallback( editor );

    if ( promptBox.open() == 0 && editor.modified ) {
      model.updateParameterMap();
      model.setSelectedParameter( parameterGeneration.getParameterName() );
    }
  }

  public void removeParameter() throws XulException {
    PushDownOptimizationMeta meta = checkNotNull( model.getSelectedOptimization() );
    String parameterName = ( (ParameterGeneration) meta.getType() ).getParameterName();

    XulMessageBox messageBox = createMessageBox();
    messageBox.setTitle( getString( PKG, "ParameterGenerationController.Delete.Title" ) );
    messageBox.setMessage( getString( PKG, "ParameterGenerationController.Delete.Message", parameterName ) );
    messageBox.setIcon( SWT.ICON_QUESTION );
    messageBox.setButtons( new Object[] { SWT.YES, SWT.NO } );

    if ( messageBox.open() == SWT.YES ) {
      model.setSelectedParameter( null );
      model.remove( meta );
    }
  }

  private class ParameterEditor implements XulDialogCallback<String> {
    private final ParameterGeneration parameterGeneration;
    private boolean modified;

    public ParameterEditor( ParameterGeneration parameterGeneration ) {
      this.parameterGeneration = parameterGeneration;
      modified = false;
    }

    @Override public void onClose( XulComponent sender, Status returnCode, String retVal ) {
      try {
        if ( returnCode == Status.ACCEPT && validate( retVal ) ) {
          parameterGeneration.setParameterName( retVal );
          modified = true;
        }
      } catch ( XulException e ) {
        onError( sender, e );
      }
    }

    private boolean validate( String parameterName ) throws XulException {

      if ( Strings.isNullOrEmpty( parameterName ) ) {
        error( getString( PKG, "ParameterGenerationController.NameMissing.Title" ),
          getString( PKG, "ParameterGenerationController.NameMissing.Message" ) );
        return false;
      }
      if ( parameterName.equals( parameterGeneration.getParameterName() ) ) {
        // No change, return false to perform no update. No Error thrown
        return false;
      }
      if ( model.getParameterMap().containsKey( parameterName ) ) {
        error( getString( PKG, "ParameterGenerationController.NameExist.Title" ),
          getString( PKG, "ParameterGenerationController.NameExist.Message" ) );
        return false;
      }
      return true;
    }

    @Override public void onError( XulComponent sender, Throwable t ) {
      getLogChannel().logError( "Failed to modify Parameter Generation", t );
    }
  }
}
