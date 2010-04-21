package org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.controller;

/*
 * This program is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
 * Foundation.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
 * or from the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 *
 * Copyright (c) 2009 Pentaho Corporation.  All rights reserved.
 */

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Map.Entry;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.i18n.GlobalMessages;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.IUIRole;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.IUIAbsRole;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.model.UIAbsSecurity;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.controller.EESecurityController;
import org.pentaho.di.ui.repository.pur.services.IAbsSecurityManager;
import org.pentaho.di.ui.repository.repositoryexplorer.ControllerInitializationException;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingConvertor;
import org.pentaho.ui.xul.components.XulButton;
import org.pentaho.ui.xul.components.XulCheckbox;
import org.pentaho.ui.xul.components.XulMessageBox;
import org.pentaho.ui.xul.containers.XulListbox;
import org.pentaho.ui.xul.containers.XulVbox;

/**
 *
 * This is the XulEventHandler for the browse panel of the repository explorer. It sets up the bindings for  
 * browse functionality.
 * 
 */
public class AbsController extends EESecurityController {
  private boolean initialized = false;

  private ResourceBundle messages = new ResourceBundle() {

    @Override
    public Enumeration<String> getKeys() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected Object handleGetObject(String key) {
      return BaseMessages.getString(IUIAbsRole.class, key);
    }

  };

  private XulButton applyLogicalRolesButton;

  private XulVbox logicalRolesBox;

  private XulListbox roleListBox;

  Map<XulCheckbox, String> logicalRoleChecboxMap = new HashMap<XulCheckbox, String>();

  private BindingConvertor<Integer, Boolean> buttonConverter;

  private IAbsSecurityManager service;

  private UIAbsSecurity absSecurity;

  public AbsController() {

  }

  @Override
  public void init(Repository rep) throws ControllerInitializationException {
    if (!initialized) {
      try {
        if (rep.hasService(IAbsSecurityManager.class)) {
          service = (IAbsSecurityManager) rep.getService(IAbsSecurityManager.class);
          try {
            service.initialize(GlobalMessages.getLocale().getDisplayName());
          } catch (Throwable th) {
            service.initialize("en_us"); //$NON-NLS-1$
          }
        } else {
          throw new ControllerInitializationException(BaseMessages.getString(IUIAbsRole.class,
              "AbsController.ERROR_0001_UNABLE_TO_INITIAL_REPOSITORY_SERVICE", IAbsSecurityManager.class)); //$NON-NLS-1$
        }
      } catch (KettleException ke) {
        throw new ControllerInitializationException(ke);
      }

      super.init(rep);
      initialized = true;
    }
  }

  @Override
  protected void setInitialDeck() {
    super.setInitialDeck();
    initializeLogicalRolesUI();
  }

  protected void createSecurity() throws Exception {
    security = eeSecurity = absSecurity = new UIAbsSecurity(service);
  }

  @Override
  protected void createBindings() {
    super.createBindings();
    roleListBox = (XulListbox) document.getElementById("roles-list");//$NON-NLS-1$
    applyLogicalRolesButton = (XulButton) document.getElementById("apply-action-permission");//$NON-NLS-1$
    logicalRolesBox = (XulVbox) document.getElementById("role-action-permissions-vbox");//$NON-NLS-1$
    bf.setBindingType(Binding.Type.ONE_WAY);
    // Action based security permissions
    buttonConverter = new BindingConvertor<Integer, Boolean>() {

      @Override
      public Boolean sourceToTarget(Integer value) {
        if (value != null && value >= 0) {
          return false;
        }
        return true;
      }

      @Override
      public Integer targetToSource(Boolean value) {
        // TODO Auto-generated method stub
        return null;
      }
    };

    bf.createBinding(roleListBox, "selectedIndex", applyLogicalRolesButton, "disabled", buttonConverter);//$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(absSecurity, "selectedRole", this, "selectedRoleChanged");//$NON-NLS-1$ //$NON-NLS-2$

  }

  /**
   * Update the model with the current status
   */
  public void updateRoleActionPermission() {
    for (Entry<XulCheckbox, String> currentEntry : logicalRoleChecboxMap.entrySet()) {
      XulCheckbox permissionCheckbox = currentEntry.getKey();
      if (permissionCheckbox.isChecked()) {
        absSecurity.addLogicalRole(currentEntry.getValue());
      } else {
        absSecurity.removeLogicalRole(currentEntry.getValue());
      }
    }
  }

  /**
   * Save the permission for the selected role
   */
  public void applyRoleActionPermission() {
    XulMessageBox messageBox = this.getMessageBox();
    IUIRole role = null;
    IUIAbsRole absRole = null;
    try {
      role = absSecurity.getSelectedRole();
      if (role instanceof IUIAbsRole) {
        absRole = (IUIAbsRole) role;
      } else {
        throw new IllegalStateException();
      }
      service.setLogicalRoles(absRole.getName(), absRole.getLogicalRoles());
      messageBox.setTitle(messages.getString("Dialog.Success"));//$NON-NLS-1$
      messageBox.setAcceptLabel(messages.getString("Dialog.Ok"));//$NON-NLS-1$
      messageBox.setMessage(messages.getString("AbsController.RoleActionPermission.Success"));//$NON-NLS-1$
      messageBox.open();
    } catch (KettleException e) {
      messageBox.setTitle(messages.getString("Dialog.Error"));//$NON-NLS-1$
      messageBox.setAcceptLabel(messages.getString("Dialog.Ok"));//$NON-NLS-1$
      messageBox.setMessage(BaseMessages.getString(IUIAbsRole.class,
          "AbsController.RoleActionPermission.UnableToApplyPermissions", role.getName(), e.getLocalizedMessage()));//$NON-NLS-1$
      messageBox.open();
    }
  }

  /**
   * The method is called when a user select a role from the role list. This method reads the current selected
   * role and populates the Action Permission UI with the details
   */
  public void setSelectedRoleChanged(IUIRole role) throws Exception {
    IUIAbsRole absRole = null;
    uncheckAllActionPermissions();
    if (role instanceof IUIAbsRole) {
      absRole = (IUIAbsRole) role;
      if (absRole != null && absRole.getLogicalRoles() != null) {
        for (String permission : absRole.getLogicalRoles()) {
          XulCheckbox permissionCheckbox = findCheckbox(permission);
          if (permissionCheckbox != null) {
            permissionCheckbox.setChecked(true);
          }
        }
      }
    } else {
      throw new IllegalStateException();
    }
  }

  private XulCheckbox findCheckbox(String permission) {
    for (Entry<XulCheckbox, String> currentEntry : logicalRoleChecboxMap.entrySet()) {
      if (currentEntry.getValue().equals(permission)) {
        return currentEntry.getKey();
      }
    }
    return null;
  }

  /**
   * Initialized the ActionPermissions UI with all the possible values from  LogicalRoles enum
   */
  private void initializeLogicalRolesUI() {
    try {
      Map<String, String> logicalRoles = service.getAllLogicalRoles(GlobalMessages.getLocale().getDisplayName());
      for (Entry<String, String> logicalRole : logicalRoles.entrySet()) {
        if (!exists(logicalRole.getValue())) {
          XulCheckbox logicalRoleCheckbox;
          logicalRoleCheckbox = (XulCheckbox) document.createElement("checkbox");//$NON-NLS-1$
          logicalRoleCheckbox.setLabel(logicalRole.getValue());
          logicalRoleCheckbox.setId(logicalRole.getValue());
          logicalRoleCheckbox.setCommand("iSecurityController.updateRoleActionPermission()");//$NON-NLS-1$
          logicalRoleCheckbox.setFlex(1);
          logicalRoleCheckbox.setDisabled(true);
          logicalRolesBox.addChild(logicalRoleCheckbox);
          logicalRoleChecboxMap.put(logicalRoleCheckbox, logicalRole.getKey());
          bf.setBindingType(Binding.Type.ONE_WAY);
          bf.createBinding(roleListBox, "selectedIndex", logicalRoleCheckbox, "disabled", buttonConverter);//$NON-NLS-1$ //$NON-NLS-2$
        }
      }
    } catch (XulException xe) {

    } catch (KettleException xe) {

    }

  }

  private boolean exists(String id) {
    for (XulComponent component : logicalRolesBox.getChildNodes()) {
      if (component.getId() != null && component.getId().equals(id)) {
        return true;
      }
    }
    return false;
  }

  private void uncheckAllActionPermissions() {
    for (XulCheckbox permissionCheckbox : logicalRoleChecboxMap.keySet()) {
      permissionCheckbox.setChecked(false);
    }
  }
}
