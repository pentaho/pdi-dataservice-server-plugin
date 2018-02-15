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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.components.XulCheckbox;
import org.pentaho.ui.xul.dom.Document;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link StreamingController} test class
 */
@RunWith( MockitoJUnitRunner.class )
public class StreamingControllerTest {
  private StreamingController streamingController;

  @Mock DataServiceModel model;
  @Mock XulCheckbox checkbox;
  @Mock XulDomContainer xulDomContainer;
  @Mock Document document;

  @Before
  public void setup() throws Exception {
    streamingController = new StreamingController( );

    when( xulDomContainer.getDocumentRoot() ).thenReturn( document );
    when( document.getElementById( anyString() ) ).thenReturn( checkbox );

    streamingController.setXulDomContainer( xulDomContainer );
  }

  @Test
  public void testInitBindings() {
    streamingController.initBindings( model );

    verify( model ).isStreaming();
  }
}
