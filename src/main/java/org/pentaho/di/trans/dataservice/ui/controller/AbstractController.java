package org.pentaho.di.trans.dataservice.ui.controller;

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
public class AbstractController extends AbstractXulEventHandler {

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

  protected BindingFactory createBindingFactory() {
    DefaultBindingFactory bindingFactory = new DefaultBindingFactory();
    bindingFactory.setDocument( document );
    return bindingFactory;
  }
}
