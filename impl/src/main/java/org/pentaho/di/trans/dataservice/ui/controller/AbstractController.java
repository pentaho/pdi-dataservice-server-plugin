/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.ui.controller;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.eclipse.swt.SWT;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.binding.DefaultBindingFactory;
import org.pentaho.ui.xul.components.XulMessageBox;
import org.pentaho.ui.xul.components.XulPromptBox;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

/**
 * @author nhudak
 */
public abstract class AbstractController extends AbstractXulEventHandler {

  private Supplier<BindingFactory> bindingFactorySupplier = new Supplier<BindingFactory>() {
    @Override public BindingFactory get() {
      DefaultBindingFactory bindingFactory = new DefaultBindingFactory();
      bindingFactory.setDocument( document );
      return bindingFactory;
    }
  };

  @SuppressWarnings( "unchecked" ) public <T extends XulComponent> T getElementById( String id ) {
    return (T) document.getElementById( id );
  }

  public XulPromptBox createPromptBox() throws XulException {
    XulPromptBox promptBox = (XulPromptBox) document.createElement( "promptbox" );
    promptBox.setModalParent( xulDomContainer.getOuterContext() );
    return promptBox;
  }

  public XulMessageBox createMessageBox() throws XulException {
    XulMessageBox messageBox = (XulMessageBox) document.createElement( "messagebox" );
    messageBox.setModalParent( xulDomContainer.getOuterContext() );
    return messageBox;
  }

  public void setBindingFactory( BindingFactory bindingFactory ) {
    this.bindingFactorySupplier = Suppliers.ofInstance( bindingFactory );
  }

  public BindingFactory getBindingFactory() {
    return bindingFactorySupplier.get();
  }

  protected void info( String title, String message ) throws XulException {
    XulMessageBox messageBox = createMessageBox();
    messageBox.setTitle( title );
    messageBox.setMessage( message );
    messageBox.setIcon( SWT.ICON_INFORMATION );
    messageBox.setButtons( new Object[] { SWT.OK } );
    messageBox.open();
  }

  protected void error( String title, String message ) throws XulException {
    XulMessageBox messageBox = createMessageBox();
    messageBox.setTitle( title );
    messageBox.setMessage( message );
    messageBox.setIcon( SWT.ICON_WARNING );
    messageBox.setButtons( new Object[] { SWT.OK } );
    messageBox.open();
  }

  protected LogChannelInterface getLogChannel() {
    return LogChannel.UI;
  }
}
