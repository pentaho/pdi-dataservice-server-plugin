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
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.components.XulCheckbox;

public class StreamingController extends AbstractController {
  private static final String NAME = "streamingCtrl";

  public StreamingController( ) {
    setName( NAME );
  }

  public void initBindings( DataServiceModel model ) {
    BindingFactory bindingFactory = getBindingFactory();

    if ( document != null ) {
      XulCheckbox checkbox = getElementById( "streaming-checkbox" );

      bindingFactory.setBindingType( Binding.Type.ONE_WAY );

      checkbox.setChecked( model.isStreaming() );
      bindingFactory.createBinding( checkbox, "checked", model, "streaming" );
    }
  }
}
