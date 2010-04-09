package org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.controller;

import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.repository.pur.services.IAbsSecurityProvider;
import org.pentaho.di.ui.repository.repositoryexplorer.ControllerInitializationException;
import org.pentaho.di.ui.repository.repositoryexplorer.controllers.PartitionsController;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIPartition;

public class AbsPartitionsController extends PartitionsController{
  IAbsSecurityProvider service;
  boolean isAllowed = false;
  @Override
  public void init(Repository repository) throws ControllerInitializationException{
    super.init(repository);
    try {
      if(repository.hasService(IAbsSecurityProvider.class)) {
        service = (IAbsSecurityProvider) repository.getService(IAbsSecurityProvider.class);
        setAllowed(allowedActionsContains(service, IAbsSecurityProvider.CREATE_CONTENT_ACTION));
      }
    } catch (KettleException e) {
      throw new ControllerInitializationException(e);
    }
  }

  @Override
  public void setEnableButtons(List<UIPartition> partitions) {
      if(isAllowed) {
        super.setEnableButtons(partitions);
      } else {
        enableButtons(false, false, false);
      }
  }
  
  
  public boolean isAllowed() {
    return isAllowed;
  }

  public void setAllowed(boolean isAllowed) {
    this.isAllowed = isAllowed;
    this.firePropertyChange("allowed", null, isAllowed);
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