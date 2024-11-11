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


package org.pentaho.di.trans.dataservice.streaming.ui;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.dataservice.ui.model.DataServiceModel;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingConvertor;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.components.XulRadio;
import org.pentaho.ui.xul.components.XulTab;
import org.pentaho.ui.xul.components.XulTextbox;
import org.pentaho.ui.xul.dom.Document;

import java.lang.reflect.InvocationTargetException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link StreamingController} test class
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class)
public class StreamingControllerTest {
  private StreamingController controller;
  private String NAME = "streamingCtrl";

  @Mock XulDomContainer xulDomContainer;
  @Mock Document document;
  @Mock BindingFactory bindingFactory;
  @Mock XulTextbox maxRows;
  @Mock XulTextbox maxTime;
  @Mock DataServiceModel model;
  @Mock XulRadio streamingRadioButton;
  @Mock XulRadio regularRadioButton;
  @Mock XulTab streamingTab;
  @Mock Binding binding;

  @Before
  public void setup() {
    controller = new StreamingController( );
    when( xulDomContainer.getDocumentRoot() ).thenReturn( document );

    when( document.getElementById( "streaming-type-radio" ) ).thenReturn( streamingRadioButton );
    when( document.getElementById( "regular-type-radio" ) ).thenReturn( regularRadioButton );
    when( document.getElementById( "streaming-tab" ) ).thenReturn( streamingTab );
    when( document.getElementById( "streaming-max-rows" ) ).thenReturn( maxRows );
    when( document.getElementById( "streaming-max-time" ) ).thenReturn( maxTime );

    when( model.isStreaming() ).thenReturn( true );

    when( bindingFactory.createBinding( model, "serviceMaxRows", maxRows, "value",
      BindingConvertor.integer2String() ) ).thenReturn( binding );
    when( bindingFactory.createBinding( model, "serviceMaxTime", maxTime, "value",
      BindingConvertor.long2String() ) ).thenReturn( binding );

    controller.setXulDomContainer( xulDomContainer );
    controller.setBindingFactory( bindingFactory );
  }

  @Test
  public void testSetName() {
    assertEquals( NAME, controller.getName() );
  }

  @Test
  public void testInitBindings() throws InvocationTargetException, XulException, KettleException {
    controller.initBindings( model );

    verify( streamingTab ).setVisible( true );

    verify( bindingFactory ).createBinding( model, "serviceMaxRows", maxRows, "value",
      BindingConvertor.integer2String() );
    verify( bindingFactory ).createBinding( model, "serviceMaxTime", maxTime, "value",
      BindingConvertor.long2String() );
    verify( bindingFactory ).createBinding( streamingRadioButton, "selected", streamingTab,
      "visible" );
    verify( bindingFactory ).createBinding( regularRadioButton, "!selected", streamingTab,
      "visible" );
  }

  @Test
  public void testInitBindingsUsingKettleProperties() throws InvocationTargetException, XulException, KettleException {
    int maxRowsValue = 5000;
    long maxTimeValue = 1000;
    when( model.getServiceMaxRows() ).thenReturn( maxRowsValue );
    when( model.getServiceMaxTime() ).thenReturn( maxTimeValue );
    System.setProperty( "KETTLE_STREAMING_ROW_LIMIT", "5000" );
    System.setProperty( "KETTLE_STREAMING_TIME_LIMIT", "10000" );
    controller.initBindings( model );

    verify( streamingTab ).setVisible( true );

    verify( maxRows ).setValue( Integer.toString( maxRowsValue ) );
    verify( maxTime ).setValue( Long.toString( maxTimeValue ) );

    verify( bindingFactory ).createBinding( model, "serviceMaxRows", maxRows, "value",
        BindingConvertor.integer2String() );
    verify( bindingFactory ).createBinding( model, "serviceMaxTime", maxTime, "value",
        BindingConvertor.long2String() );
    verify( bindingFactory ).createBinding( streamingRadioButton, "selected", streamingTab,
        "visible" );
    verify( bindingFactory ).createBinding( regularRadioButton, "!selected", streamingTab,
      "visible" );
  }
}
