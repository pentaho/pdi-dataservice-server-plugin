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

package org.pentaho.di.trans.dataservice.optimization.pushdown.ui;

import org.pentaho.di.trans.dataservice.ui.controller.AbstractController;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;

/**
 * @author nhudak
 */
public class ParameterPushdownController extends AbstractController {
  private static final Class<?> PKG = ParameterPushdownController.class;
  public static final String NAME = "pushDownParam";

  {
    setName( NAME );
  }

  private final ParameterPushdownModel model;

  public ParameterPushdownController( ParameterPushdownModel model ) {
    this.model = model;
  }

  @Override public void setXulDomContainer( XulDomContainer xulDomContainer ) {
    super.setXulDomContainer( xulDomContainer );
    // When the container is set, initialize the model bindings
    initBindings();
  }

  private void initBindings() {
    BindingFactory bindingFactory = getBindingFactory();

    bindingFactory.setBindingType( Binding.Type.ONE_WAY );
    bindingFactory.createBinding( model, "definitions", "param_definitions", "elements" );

    model.reset();
  }

  public ParameterPushdownModel getModel() {
    return model;
  }
}
