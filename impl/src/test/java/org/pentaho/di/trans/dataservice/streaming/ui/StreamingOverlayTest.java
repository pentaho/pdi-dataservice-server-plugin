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


package org.pentaho.di.trans.dataservice.streaming.ui;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
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
@RunWith( MockitoJUnitRunner.StrictStubs.class)
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
