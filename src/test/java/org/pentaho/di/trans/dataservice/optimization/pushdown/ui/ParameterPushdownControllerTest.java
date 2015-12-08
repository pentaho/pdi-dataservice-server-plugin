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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;

import static org.mockito.Mockito.inOrder;

/**
 * @author nhudak
 */

@RunWith( org.mockito.runners.MockitoJUnitRunner.class )
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
