/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
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
