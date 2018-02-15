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
import org.pentaho.di.trans.dataservice.ui.DataServiceDialog;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.ui.xul.XulDomContainer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doNothing;

/**
 * {@link StreamingOverlay} test class
 */
@RunWith( MockitoJUnitRunner.class )
public class StreamingOverlayTest {
  private StreamingOverlay streamingOverlay;

  @Mock DataServiceDialog dialog;
  @Mock DataServiceModel model;
  @Mock XulDomContainer domContainer;

  @Before
  public void setup() throws Exception {
    streamingOverlay = new StreamingOverlay( );

    when( dialog.getModel() ).thenReturn( model );
    when( dialog.applyOverlay( anyObject(), anyString() ) ).thenReturn( domContainer );
  }

  @Test
  public void testGetPriority() {
    assertThat( 3.0, equalTo( streamingOverlay.getPriority() ) );
  }

  @Test
  public void testApply() throws Exception {
    streamingOverlay.apply( dialog );
    verify( dialog ).getModel();
  }
}
