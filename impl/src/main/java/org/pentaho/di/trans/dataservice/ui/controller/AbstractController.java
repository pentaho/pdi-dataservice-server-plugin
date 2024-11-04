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
