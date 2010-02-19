package org.pentaho.di.ui.repository.repositoryexplorer.abs.controller;

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
import org.pentaho.di.repository.AbsSecurityManager;
import org.pentaho.di.ui.repository.repositoryexplorer.ControllerInitializationException;
import org.pentaho.di.ui.repository.repositoryexplorer.RepositoryExplorer;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.AbsSpoonPlugin;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.model.UIAbsRepositoryRole;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.model.UIAbsSecurity;
import org.pentaho.di.ui.repository.repositoryexplorer.controllers.SecurityController;
import org.pentaho.di.ui.repository.repositoryexplorer.model.IUIRole;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryRole;
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
  public class AbsController  extends SecurityController {

    private ResourceBundle messages = new ResourceBundle() {

      @Override
      public Enumeration<String> getKeys() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      protected Object handleGetObject(String key) {
        return BaseMessages.getString(AbsSpoonPlugin.class, key);
      }
      
    }; 

    private XulButton applyLogicalRolesButton;

    private XulVbox logicalRolesBox;
    
    private XulListbox roleListBox;

    Map<XulCheckbox, String> logicalRoleChecboxMap;

    private BindingConvertor<Integer, Boolean> buttonConverter = null;
    private AbsSecurityManager absAdmin  = null; 
    public AbsController() {

    }

    
    public void init() throws ControllerInitializationException{
      try {
        absAdmin  = (AbsSecurityManager) this.getRepositorySecurityManager();
        absAdmin.initialize(GlobalMessages.getLocale().getDisplayName());
        super.init();
        initializeLogicalRolesUI();
      } catch (Exception e) {
        throw new ControllerInitializationException(e);
      }
    }

    
    @Override
    protected void createModel() {
      security = new UIAbsSecurity(getRepositorySecurityManager());
    }


    protected void createBindings() {
      super.createBindings();
      roleListBox = (XulListbox) document.getElementById("roles-list");//$NON-NLS-1$
      applyLogicalRolesButton = (XulButton) document.getElementById("apply-action-permission");//$NON-NLS-1$
      logicalRolesBox = (XulVbox) document.getElementById("role-action-permissions-vbox");//$NON-NLS-1$
        this.getBindingFactory().setBindingType(Binding.Type.ONE_WAY);
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
        
        this.getBindingFactory().createBinding(roleListBox, "selectedIndex", applyLogicalRolesButton, "disabled", buttonConverter);//$NON-NLS-1$ //$NON-NLS-2$
        this.getBindingFactory().createBinding(getSecurity(), "selectedRole", this, "selectedRoleChanged");//$NON-NLS-1$ //$NON-NLS-2$

    }
    
    public String getName() {
      return "iSecurityController"; //$NON-NLS-1$
    }
    
    /**
     * Update the model with the current status
     */
    public void updateRoleActionPermission() {
      for(Entry<XulCheckbox, String> currentEntry:logicalRoleChecboxMap.entrySet()) {
        XulCheckbox permissionCheckbox = currentEntry.getKey();
        if(permissionCheckbox.isChecked()) {
          ((UIAbsSecurity)getSecurity()).addLogicalRole(currentEntry.getValue());
        } else {
          ((UIAbsSecurity)getSecurity()).removeLogicalRole(currentEntry.getValue());
        }
      }
    }

    /**
     * Save the permission for the selected role
     */
    public void applyRoleActionPermission() {
      XulMessageBox messageBox = this.getMessageBox();
      IUIRole role = null;
      try {
        role = getSecurity().getSelectedRole();
        absAdmin.setLogicalRoles(role.getName(), ((UIAbsRepositoryRole) getSecurity()
            .getSelectedRole()).getLogicalRoles());
        messageBox.setTitle(messages.getString("Dialog.Success"));//$NON-NLS-1$
        messageBox.setAcceptLabel(messages.getString("Dialog.Ok"));//$NON-NLS-1$
        messageBox.setMessage(messages.getString("AbsController.RoleActionPermission.Success"));//$NON-NLS-1$
        messageBox.open();

      } catch (KettleException e) {
        messageBox.setTitle(messages.getString("Dialog.Error"));//$NON-NLS-1$
        messageBox.setAcceptLabel(messages.getString("Dialog.Ok"));//$NON-NLS-1$
        messageBox.setMessage(BaseMessages.getString(RepositoryExplorer.class,
            "AbsController.RoleActionPermission.UnableToApplyPermissions", role.getName(), e.getLocalizedMessage()));//$NON-NLS-1$
        messageBox.open();
      }
    }

    /**
     * The method is called when a user select a role from the role list. This method reads the current selected
     * role and populates the Action Permission UI with the details
     */
    public void setSelectedRoleChanged(UIRepositoryRole role) throws Exception {
      uncheckAllActionPermissions();
      if (role != null && ((UIAbsRepositoryRole)role).getLogicalRoles() != null) {
        for (String permission : ((UIAbsRepositoryRole)role).getLogicalRoles()) {
          XulCheckbox permissionCheckbox = findCheckbox(permission);
          if(permissionCheckbox != null) {
            permissionCheckbox.setChecked(true);
          }
        }
      }
    }

    private XulCheckbox findCheckbox(String permission) {
      for(Entry<XulCheckbox, String> currentEntry:logicalRoleChecboxMap.entrySet()) {
        if(currentEntry.getValue().equals(permission)) {
          return currentEntry.getKey();
        }
      }
      return null;
    }
    
    /**
     * Initialized the ActionPermissions UI with all the possible values from  LogicalRoles enum
     */
    private void initializeLogicalRolesUI() {
      logicalRoleChecboxMap = new HashMap<XulCheckbox, String>();
      try {
        Map<String, String> logicalRoles = absAdmin.getAllLogicalRoles(GlobalMessages.getLocale().getDisplayName());
        for (Entry<String, String> logicalRole:logicalRoles.entrySet()) {
          XulCheckbox logicalRoleCheckbox;
          logicalRoleCheckbox = (XulCheckbox) document.createElement("checkbox");//$NON-NLS-1$
          logicalRoleCheckbox.setLabel(logicalRole.getValue());
          logicalRoleCheckbox.setId(logicalRole.getValue());
          logicalRoleCheckbox.setCommand("iSecurityController.updateRoleActionPermission()");//$NON-NLS-1$
          logicalRoleCheckbox.setFlex(1);
          logicalRoleCheckbox.setDisabled(true);
          logicalRolesBox.addChild(logicalRoleCheckbox);
          logicalRoleChecboxMap.put(logicalRoleCheckbox,logicalRole.getKey());
          this.getBindingFactory().setBindingType(Binding.Type.ONE_WAY);
          this.getBindingFactory().createBinding(roleListBox, "selectedIndex", logicalRoleCheckbox, "disabled", buttonConverter);//$NON-NLS-1$ //$NON-NLS-2$
        }
      } catch (XulException xe) {

      } catch (KettleException xe) {

      }

    }

    private void uncheckAllActionPermissions() {
      for(XulCheckbox permissionCheckbox:logicalRoleChecboxMap.keySet()) {
        permissionCheckbox.setChecked(false);
      }
    }
}
