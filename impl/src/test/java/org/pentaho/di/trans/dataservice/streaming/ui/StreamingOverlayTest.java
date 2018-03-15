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
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.dataservice.ui.DataServiceDialog;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;

import java.lang.reflect.InvocationTargetException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link StreamingOverlay} test class
 */
@RunWith( MockitoJUnitRunner.class )
public class StreamingOverlayTest {
  private StreamingOverlay streamingOverlay;
  private String XUL_OVERLAY =
    "/org/pentaho/di/trans/dataservice/streaming/ui/streaming-overlay.xul";

  @Mock private DataServiceDialog dialog;
  @Mock private DataServiceModel model;
  @Mock private XulDomContainer container;
  @Mock private StreamingController controller;

  @Before
  public void setup() throws Exception {
    streamingOverlay = new StreamingOverlay( controller );
    when( model.isStreaming() ).thenReturn( true );
    when( dialog.applyOverlay( streamingOverlay, XUL_OVERLAY ) ).thenReturn( container );
    when( dialog.getModel() ).thenReturn( model );
  }

  @Test
  public void testGePriority() {
    assertEquals( streamingOverlay.getPriority(),  0, 0  );
  }

  @Test
  public void testApply() throws KettleException, InvocationTargetException, XulException {
    streamingOverlay.apply( dialog );
    verify( controller ).initBindings( model );
  }

  @Test ( expected = KettleException.class )
  public void testApplyException() throws KettleException, InvocationTargetException, XulException {
    doThrow( new XulException() ).when( controller ).initBindings( model );
    streamingOverlay.apply( dialog );
  }
}
