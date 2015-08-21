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

package org.pentaho.di.trans.dataservice.optimization.paramgen.ui;

import com.google.common.base.Strings;
import org.eclipse.swt.SWT;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGeneration;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGenerationFactory;
import org.pentaho.di.trans.dataservice.ui.controller.AbstractController;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingConvertor;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.components.XulMenuList;
import org.pentaho.ui.xul.components.XulMessageBox;
import org.pentaho.ui.xul.components.XulPromptBox;
import org.pentaho.ui.xul.containers.XulListbox;
import org.pentaho.ui.xul.util.XulDialogCallback;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author nhudak
 */
public class ParameterGenerationController extends AbstractController {

  private static final String NAME = "paramGenCtrl";
  private static final BindingConvertor<String, Boolean> stringIsEmpty = new BindingConvertor<String, Boolean>() {
    @Override public Boolean sourceToTarget( String value ) {
      return Strings.isNullOrEmpty( value );
    }

    @Override public String targetToSource( Boolean value ) {
      throw new AbstractMethodError( "Boolean to String conversion is not supported" );
    }
  };

  {
    setName( NAME );
  }

  private final ParameterGenerationFactory factory;
  private final ParameterGenerationModel model;

  public ParameterGenerationController( ParameterGenerationFactory factory, ParameterGenerationModel model ) {
    this.factory = factory;
    this.model = model;
  }

  public void initBindings() {
    BindingFactory bindingFactory = createBindingFactory();

    final XulListbox paramGenList = getElementById( "param_gen_list" );
    XulMenuList<String> stepList = getElementById( "param_gen_step" );
    stepList.setElements( model.getSupportedSteps().keySet().asList() );

    // BI DIRECTIONAL bindings
    bindingFactory.setBindingType( Binding.Type.BI_DIRECTIONAL );
    bindingFactory.createBinding( model, "selectedParameter", paramGenList, "selectedItem" );
    bindingFactory.createBinding( model, "selectedStep", stepList, "value" );
    bindingFactory.createBinding( model, "enabled", "param_gen_enabled", "checked" );

    // ONE WAY bindings
    bindingFactory.setBindingType( Binding.Type.ONE_WAY );
    model.addPropertyChangeListener( "parameterMap", new PropertyChangeListener() {
      @Override public void propertyChange( PropertyChangeEvent evt ) {
        paramGenList.setElements( model.getParameterMap().keySet() );
        paramGenList.setSelectedItem( model.getSelectedParameter() );
      }
    } );
    bindingFactory.createBinding( model, "selectedParameter", "param_gen_edit", "disabled", stringIsEmpty );
    bindingFactory.createBinding( model, "selectedParameter", "param_gen_remove", "disabled", stringIsEmpty );
    bindingFactory.createBinding( model, "selectedParameter", "param_gen_step", "disabled", stringIsEmpty );
    bindingFactory.createBinding( model, "selectedParameter", "param_gen_mapping", "disabled", stringIsEmpty );
    bindingFactory.createBinding( model, "selectedParameter", "param_gen_enabled", "disabled", stringIsEmpty );
    bindingFactory.createBinding( model, "mappings", "param_gen_mapping", "elements" );

    model.updateParameterMap();
  }

  public void addParameter() throws XulException {
    PushDownOptimizationMeta meta = new PushDownOptimizationMeta();
    ParameterGeneration parameterGeneration = factory.createPushDown();
    meta.setType( parameterGeneration );

    XulPromptBox promptBox = createPromptBox();
    promptBox.setTitle( "Create Parameter" );
    promptBox.setMessage( "Parameter Name:" );
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
    promptBox.setTitle( "Edit Parameter" );
    promptBox.setMessage( "Parameter Name:" );
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
    messageBox.setTitle( "Remove Parameter" );
    messageBox.setMessage( "Are you sure you want to delete " + parameterName + "?" );
    messageBox.setIcon( SWT.ICON_QUESTION );
    messageBox.setButtons( new Object[] { SWT.YES, SWT.NO } );

    if ( messageBox.open() == SWT.YES ) {
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
        error( "Parameter can not be empty" );
        return false;
      }
      if ( parameterName.equals( parameterGeneration.getParameterName() ) ) {
        // No change, return false to perform no update. No Error thrown
        return false;
      }
      if ( model.getParameterMap().containsKey( parameterName ) ) {
        error( "Parameter already exists" );
        return false;
      }
      return true;
    }

    private void error( String message ) throws XulException {
      XulMessageBox messageBox = createMessageBox();
      messageBox.setTitle( "Invalid Parameter" );
      messageBox.setMessage( message );
      messageBox.setIcon( SWT.ICON_ERROR );
      messageBox.open();
    }

    @Override public void onError( XulComponent sender, Throwable t ) {
      LogChannel.UI.logError( "Failed to modify Parameter Generation", t );
    }
  }
}
