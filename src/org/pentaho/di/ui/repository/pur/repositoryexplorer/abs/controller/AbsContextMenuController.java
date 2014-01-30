/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.controller;

import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.repository.pur.services.IAbsSecurityProvider;
import org.pentaho.di.ui.repository.repositoryexplorer.ControllerInitializationException;
import org.pentaho.di.ui.repository.repositoryexplorer.IUISupportController;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.binding.DefaultBindingFactory;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

public class AbsContextMenuController extends AbstractXulEventHandler   implements IUISupportController, java.io.Serializable {

  private static final long serialVersionUID = 8878231461011554114L; /* EESOURCE: UPDATE SERIALVERUID */

  private IAbsSecurityProvider service;
  private boolean isAllowed = false;
  private BindingFactory bf;
  public void init(Repository repository) throws ControllerInitializationException {
    try {
      if(repository.hasService(IAbsSecurityProvider.class)) {
        service = (IAbsSecurityProvider) repository.getService(IAbsSecurityProvider.class);
        bf = new DefaultBindingFactory();
        bf.setDocument(this.getXulDomContainer().getDocumentRoot());

        if (bf!=null){
          createBindings();
        }
        setAllowed(allowedActionsContains(service, IAbsSecurityProvider.CREATE_CONTENT_ACTION));
      }
    } catch (KettleException e) {
      throw new ControllerInitializationException(e);
    }
  }
  
  private void createBindings() {
    bf.createBinding(this, "allowed", "file-context-rename", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    bf.createBinding(this, "allowed", "file-context-delete", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    bf.createBinding(this, "allowed", "folder-context-create", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    bf.createBinding(this, "allowed", "folder-context-rename", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    bf.createBinding(this, "allowed", "folder-context-delete", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
  }
  
  public String getName() {
    return "contextMenuController"; //$NON-NLS-1$
  }

  public boolean isAllowed() {
    return isAllowed;
  }

  public void setAllowed(boolean isAllowed) {
    this.isAllowed = isAllowed;
    this.firePropertyChange("allowed", null, isAllowed); //$NON-NLS-1$
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
