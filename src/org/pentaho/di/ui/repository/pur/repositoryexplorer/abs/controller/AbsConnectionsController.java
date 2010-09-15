package org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.controller;

import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.repository.pur.services.IAbsSecurityProvider;
import org.pentaho.di.ui.repository.repositoryexplorer.ControllerInitializationException;
import org.pentaho.di.ui.repository.repositoryexplorer.controllers.ConnectionsController;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIDatabaseConnection;

public class AbsConnectionsController extends ConnectionsController{
  IAbsSecurityProvider service;
  boolean isAllowed = false;
  
  @Override
  protected boolean doLazyInit() {
    boolean superSucceeded = super.doLazyInit();
    if (!superSucceeded) {
      return false;
    }
    try {
      if(repository.hasService(IAbsSecurityProvider.class)) {
        service = (IAbsSecurityProvider) repository.getService(IAbsSecurityProvider.class);
        setAllowed(allowedActionsContains(service, IAbsSecurityProvider.CREATE_CONTENT_ACTION));
      }
    } catch (KettleException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  public boolean isAllowed() {
    return isAllowed;
  }

  public void setAllowed(boolean isAllowed) {
    this.isAllowed = isAllowed;
    this.firePropertyChange("allowed", null, isAllowed);
  }

  @Override
  public void setEnableButtons(List<UIDatabaseConnection> connections) {
      if(isAllowed) {
        super.setEnableButtons(connections);
      } else {
        enableButtons(false, false, false);
      }
  }
  
  private boolean allowedActionsContains(IAbsSecurityProvider service, String action) throws KettleException {
    List<String> allowedActions = service.getAllowedActions(IAbsSecurityProvider.NAMESPACE);
    for (String actionName : allowedActions) {
      if (action != null && action.equals(actionName)) {
        return true;
      }
    }
    return false;
  }

}
