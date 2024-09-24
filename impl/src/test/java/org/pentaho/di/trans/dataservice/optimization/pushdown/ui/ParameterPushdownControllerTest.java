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

package org.pentaho.di.trans.dataservice.optimization.pushdown.ui;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;

import static org.mockito.Mockito.inOrder;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class ParameterPushdownControllerTest {
  @Mock ParameterPushdownModel model;
  @Mock XulDomContainer container;
  @Mock BindingFactory bindingFactory;

  @InjectMocks ParameterPushdownController controller;

  @Before
  public void setUp() throws Exception {
    controller.setBindingFactory( bindingFactory );
  }

  @Test
  public void testBindings() throws Exception {
    controller.setXulDomContainer( container );

    InOrder binding = inOrder( bindingFactory, model );
    binding.verify( bindingFactory ).setBindingType( Binding.Type.ONE_WAY );
    binding.verify( bindingFactory ).createBinding( model, "definitions", "param_definitions", "elements" );
    binding.verify( model ).reset();
  }
}
