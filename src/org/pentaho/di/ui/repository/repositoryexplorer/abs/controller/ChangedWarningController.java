package org.pentaho.di.ui.repository.repositoryexplorer.abs.controller;

import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.spoon.AbstractChangedWarningDialog;
import org.pentaho.di.ui.spoon.XulSpoonResourceBundle;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.components.XulLabel;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.dom.Document;

public class ChangedWarningController extends AbstractChangedWarningDialog {
  private static Class<?> PKG = ChangedWarningController.class;
  
  @Override
  public String getName() {
    return "changedWarningController"; //$NON-NLS-1$
  }

  @Override
  public void setXulDomContainer(XulDomContainer xulDomContainer) {
    super.setXulDomContainer(xulDomContainer);
    init();
  }
  
  protected void init() {
    Document doc = getXulDomContainer().getDocumentRoot();
    XulDialog dialog = (XulDialog)doc.getElementById(getXulDialogId());
    dialog.setButtons("extra1,cancel"); //$NON-NLS-1$
    dialog.setButtonlabelaccept(null);
    dialog.setOndialogaccept(null);
    
    XulLabel message = (XulLabel)doc.getElementById("changed-warning-dialog-message"); //$NON-NLS-1$
    message.setValue(BaseMessages.getString(PKG, "Spoon.Dialog.PromptToSave.NoSavePermission")); //$NON-NLS-1$
    
  }

  @Override
  public String getXulDialogId() {
    return "trans-graph-changed-warning-dialog"; //$NON-NLS-1$
  }

  @Override
  public String getXulResource() {
   return null;
  }
  
  @Override
  public XulSpoonResourceBundle getXulResourceBundle() {
    return null;
  }

  @Override
  public String getSpoonPluginManagerContainerNamespace() {
    return null;
  }
}
