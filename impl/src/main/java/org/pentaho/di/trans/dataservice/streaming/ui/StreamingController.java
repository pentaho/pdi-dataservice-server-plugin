/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.streaming.ui;

import org.pentaho.di.trans.dataservice.ui.controller.AbstractController;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingConvertor;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.components.XulRadio;
import org.pentaho.ui.xul.components.XulTab;
import org.pentaho.ui.xul.components.XulTextbox;

import java.lang.reflect.InvocationTargetException;

/**
 *Streaming UI controller
 */
public class StreamingController extends AbstractController {
  private static final String NAME = "streamingCtrl";

  public StreamingController() {
    setName( NAME );
  }

  /**
   * Inits the controller-ui bindings.
   *
   * @param model - The {@link org.pentaho.di.trans.dataservice.ui.model.DataServiceModel} dataservice model.
   * @throws InvocationTargetException
   * @throws XulException
   */
  public void initBindings( DataServiceModel model ) throws InvocationTargetException, XulException {
    BindingFactory bindingFactory = getBindingFactory();

    XulRadio streamingRadioButton = getElementById( "streaming-type-radio" );
    XulTab streamingTab = getElementById( "streaming-tab" );

    XulTextbox streamingMaxRows = getElementById( "streaming-max-rows" );
    XulTextbox streamingMaxTime = getElementById( "streaming-max-time" );

    bindingFactory.setBindingType( Binding.Type.BI_DIRECTIONAL );

    streamingTab.setVisible( model.isStreaming() );

    bindingFactory.createBinding( model, "serviceMaxRows", streamingMaxRows, "value",
      BindingConvertor.integer2String() ).fireSourceChanged();

    bindingFactory.createBinding( model, "serviceMaxTime", streamingMaxTime, "value",
      BindingConvertor.long2String() ).fireSourceChanged();

    bindingFactory.createBinding( streamingRadioButton, "selected", streamingTab, "visible" );
  }
}
