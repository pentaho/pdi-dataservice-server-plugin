package org.pentaho.di.ui.repository.repositoryexplorer.controller;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.IRole;
import org.pentaho.di.repository.IRoleSupportSecurityManager;
import org.pentaho.di.repository.IUser;
import org.pentaho.di.repository.ObjectRecipient;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositorySecurityManager;
import org.pentaho.di.repository.ObjectRecipient.Type;
import org.pentaho.di.ui.repository.repositoryexplorer.ControllerInitializationException;
import org.pentaho.di.ui.repository.repositoryexplorer.IUIEEUser;
import org.pentaho.di.ui.repository.repositoryexplorer.IUIRole;
import org.pentaho.di.ui.repository.repositoryexplorer.RepositoryExplorer;
import org.pentaho.di.ui.repository.repositoryexplorer.UIEEObjectRegistery;
import org.pentaho.di.ui.repository.repositoryexplorer.controllers.SecurityController;
import org.pentaho.di.ui.repository.repositoryexplorer.model.IUIUser;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIEERepositoryUser;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIEESecurity;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIEESecurityUser;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIObjectCreationException;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIObjectRegistery;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UISecurityRole;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UISecurity.Mode;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingConvertor;
import org.pentaho.ui.xul.components.XulButton;
import org.pentaho.ui.xul.components.XulConfirmBox;
import org.pentaho.ui.xul.components.XulRadio;
import org.pentaho.ui.xul.components.XulTextbox;
import org.pentaho.ui.xul.containers.XulDeck;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.containers.XulListbox;
import org.pentaho.ui.xul.containers.XulTree;
import org.pentaho.ui.xul.util.XulDialogCallback;

/**
 *
 * This is the XulEventHandler for the browse panel of the repository explorer. It sets up the bindings for  
 * browse functionality. 
 * mx
 * 
 */
public class EESecurityController extends SecurityController {
  public static final int ROLE_DECK = 1;

  private ResourceBundle messages = new ResourceBundle() {

    @Override
    public Enumeration<String> getKeys() {
      return null;
    }

    @Override
    protected Object handleGetObject(String key) {
      return BaseMessages.getString(IUIEEUser.class, key);
    }
    
  };  

  private XulRadio roleRadioButton;

  private XulRadio userRadioButton;

  private XulTree userDetailTable;

  private XulTree roleDetailTable;

  private XulDialog roleDialog;

  private XulDeck userRoleDeck;

  private XulListbox roleListBox;

  private XulListbox userListBox;

  private XulListbox availableRoles;

  private XulListbox assignedRoles;

  private XulTextbox rolename;

  private XulTextbox roleDescription;

  private XulListbox availableUsers;

  private XulListbox assignedUsers;

  private XulButton roleEditButton;

  private XulButton roleRemoveButton;

  private XulButton addUserToRoleButton;

  private XulButton removeUserFromRoleButton;

  private XulButton addRoleToUserButton;

  private XulButton removeRoleFromUserButton;

  private XulButton assignRoleToUserButton;

  private XulButton unassignRoleFromUserButton;

  private XulButton assignUserToRoleButton;

  private XulButton unassignUserFromRoleButton;
  
  private Binding roleDetailBinding;
  
  private Binding userDetailBinding;

  protected UIEESecurityUser eeSecurityUser;

  protected UISecurityRole securityRole;

  private IRoleSupportSecurityManager service;
  
  protected UIEESecurity eeSecurity;

  public EESecurityController() {
  }

  
  @Override
  public void init(Repository rep) throws ControllerInitializationException {
    try {
      if(rep.hasService(IRoleSupportSecurityManager.class)) {
        service = (IRoleSupportSecurityManager) rep.getService(IRoleSupportSecurityManager.class);
      } else {
        throw new ControllerInitializationException(BaseMessages.getString(IUIEEUser.class, "SecurityController.ERROR_0001_UNABLE_TO_INITIAL_REPOSITORY_SERVICE", IRoleSupportSecurityManager.class)); //$NON-NLS-1$
      }
    } catch (KettleException e) {
      throw new ControllerInitializationException(e);
      }
    super.init(rep);
  }


  @Override
  protected void setInitialDeck() {
    changeToRoleDeck();
  }
  
  @Override
  protected void createModel() throws Exception{
    super.createModel();
    securityRole = new UISecurityRole();
  }

  protected void createSecurityUser() throws Exception{
    securityUser = eeSecurityUser = new UIEESecurityUser(service);    
  }

  protected void createSecurity()  throws Exception {
    security = eeSecurity = new UIEESecurity(service); 
  }
  
  @Override
  protected void createBindings() {
    super.createBindings();
    //User Role Binding

    roleRadioButton = (XulRadio) document.getElementById("role-radio-button");//$NON-NLS-1$
    userRadioButton = (XulRadio) document.getElementById("user-radio-button");//$NON-NLS-1$

    roleEditButton = (XulButton) document.getElementById("role-edit");//$NON-NLS-1$
    roleRemoveButton = (XulButton) document.getElementById("role-remove");//$NON-NLS-1$

    addUserToRoleButton = (XulButton) document.getElementById("add-user-to-role");//$NON-NLS-1$
    removeUserFromRoleButton = (XulButton) document.getElementById("remove-user-from-role");//$NON-NLS-1$
    addRoleToUserButton = (XulButton) document.getElementById("add-role-to-user");//$NON-NLS-1$
    removeRoleFromUserButton = (XulButton) document.getElementById("remove-role-from-user");//$NON-NLS-1$

    roleDialog = (XulDialog) document.getElementById("add-role-dialog");//$NON-NLS-1$
    userRoleDeck = (XulDeck) document.getElementById("user-role-deck");//$NON-NLS-1$
    roleListBox = (XulListbox) document.getElementById("roles-list");//$NON-NLS-1$
    roleDetailTable = (XulTree) document.getElementById("role-detail-table");//$NON-NLS-1$

    // Add User Binding
    userListBox = (XulListbox) document.getElementById("users-list");//$NON-NLS-1$
    userDetailTable = (XulTree) document.getElementById("user-detail-table");//$NON-NLS-1$

    availableRoles = (XulListbox) document.getElementById("available-roles-list");//$NON-NLS-1$
    assignedRoles = (XulListbox) document.getElementById("selected-roles-list");//$NON-NLS-1$
    assignRoleToUserButton = (XulButton) document.getElementById("assign-role-to-user");//$NON-NLS-1$
    unassignRoleFromUserButton = (XulButton) document.getElementById("unassign-role-from-user");//$NON-NLS-1$

    bf.setBindingType(Binding.Type.BI_DIRECTIONAL);
    bf.createBinding(eeSecurityUser, "assignedRoles", assignedRoles, "elements");//$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(eeSecurityUser, "availableRoles", availableRoles, "elements");//$NON-NLS-1$ //$NON-NLS-2$

    // Binding to convert role array to a role list object and vice versa
    BindingConvertor<List<IUIRole>, Object[]> arrayToListRoleConverter = new BindingConvertor<List<IUIRole>, Object[]>() {

      @Override
      public Object[] sourceToTarget(List<IUIRole> roles) {
        if (roles != null) {
          Object[] retVal = new Object[roles.size()];
          int i = 0;
          for (IUIRole role : roles) {
            retVal[i++] = role;
          }
          return retVal;
        }
        return null;
      }

      @Override
      public List<IUIRole> targetToSource(Object[] roles) {
        if (roles != null) {
          List<IUIRole> retVal = new ArrayList<IUIRole>();
          for (int i = 0; i < roles.length; i++) {
            retVal.add((IUIRole) roles[i]);
          }
          return retVal;
        }
        return null;
      }

    };

    // Binding to convert user array to a user list object and vice versa
    BindingConvertor<List<IUIUser>, Object[]> arrayToListUserConverter = new BindingConvertor<List<IUIUser>, Object[]>() {

      @Override
      public Object[] sourceToTarget(List<IUIUser> users) {
        if (users != null) {
          Object[] retVal = new Object[users.size()];
          int i = 0;
          for (IUIUser user : users) {
            retVal[i++] = user;
          }
          return retVal;
        }
        return null;
      }

      @Override
      public List<IUIUser> targetToSource(Object[] users) {
        if (users != null) {
          List<IUIUser> retVal = new ArrayList<IUIUser>();
          for (int i = 0; i < users.length; i++) {
            retVal.add((IUIUser) users[i]);
          }
          return retVal;
        }
        return null;
      }

    };

    
    bf.createBinding(eeSecurityUser, "availableSelectedRoles", availableRoles, "selectedItems", arrayToListRoleConverter);//$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(eeSecurityUser, "assignedSelectedRoles", assignedRoles, "selectedItems", arrayToListRoleConverter);//$NON-NLS-1$ //$NON-NLS-2$

    BindingConvertor<Integer, Boolean> accumulatorButtonConverter = new BindingConvertor<Integer, Boolean>() {

      @Override
      public Boolean sourceToTarget(Integer value) {
        if (value != null && value >= 0) {
          return true;
        }
        return false;
      }

      @Override
      public Integer targetToSource(Boolean value) {
        return null;
      }
    };

    bf.setBindingType(Binding.Type.ONE_WAY);
    bf.createBinding(assignedRoles,
        "selectedIndex", eeSecurityUser, "roleUnassignmentPossible", accumulatorButtonConverter);//$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(availableRoles,
        "selectedIndex", eeSecurityUser, "roleAssignmentPossible", accumulatorButtonConverter);//$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(eeSecurityUser, "roleUnassignmentPossible", unassignRoleFromUserButton, "!disabled");//$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(eeSecurityUser, "roleAssignmentPossible", assignRoleToUserButton, "!disabled");//$NON-NLS-1$ //$NON-NLS-2$

    // Add Role Binding
    rolename = (XulTextbox) document.getElementById("role-name");//$NON-NLS-1$
    roleDescription = (XulTextbox) document.getElementById("role-description");//$NON-NLS-1$
    availableUsers = (XulListbox) document.getElementById("available-users-list");//$NON-NLS-1$
    assignedUsers = (XulListbox) document.getElementById("selected-users-list");//$NON-NLS-1$
    assignUserToRoleButton = (XulButton) document.getElementById("assign-user-to-role");//$NON-NLS-1$
    unassignUserFromRoleButton = (XulButton) document.getElementById("unassign-user-from-role");//$NON-NLS-1$

    bf.setBindingType(Binding.Type.BI_DIRECTIONAL);
    bf.createBinding(securityRole, "name", rolename, "value");//$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(securityRole, "description", roleDescription, "value");//$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(securityRole, "assignedUsers", assignedUsers, "elements");//$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(securityRole, "availableUsers", availableUsers, "elements"); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(securityRole, "availableSelectedUsers", availableUsers, "selectedItems", arrayToListUserConverter);//$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(securityRole, "assignedSelectedUsers", assignedUsers, "selectedItems", arrayToListUserConverter);//$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(eeSecurity, "selectedRoleIndex", roleListBox, "selectedIndex");//$NON-NLS-1$ //$NON-NLS-2$

    bf.setBindingType(Binding.Type.ONE_WAY);
    bf.createBinding(assignedUsers,
        "selectedIndex", securityRole, "userUnassignmentPossible", accumulatorButtonConverter);//$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(availableUsers,
        "selectedIndex", securityRole, "userAssignmentPossible", accumulatorButtonConverter); //$NON-NLS-1$ //$NON-NLS-2$  
    bf.createBinding(securityRole, "userUnassignmentPossible", unassignUserFromRoleButton, "!disabled");//$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(securityRole, "userAssignmentPossible", assignUserToRoleButton, "!disabled");//$NON-NLS-1$ //$NON-NLS-2$

    try {
      bf.setBindingType(Binding.Type.ONE_WAY);

      BindingConvertor<Integer, Boolean> buttonConverter = new BindingConvertor<Integer, Boolean>() {

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
      BindingConvertor<Object, Boolean> removeButtonConverter = new BindingConvertor<Object, Boolean>() {

        @Override
        public Boolean sourceToTarget(Object value) {
          if (value != null) {
            return false;
          }
          return true;
        }

        @Override
        public Object targetToSource(Boolean value) {
          // TODO Auto-generated method stub
          return null;
        }
      };
      bf.createBinding(roleListBox, "selectedIndex", roleEditButton, "disabled", buttonConverter);//$NON-NLS-1$ //$NON-NLS-2$
      bf.createBinding(roleListBox, "selectedIndex", roleRemoveButton, "disabled", buttonConverter);//$NON-NLS-1$ //$NON-NLS-2$

      bf.setBindingType(Binding.Type.ONE_WAY);
      // Action based security permissions
      bf.createBinding(roleListBox, "selectedItem", eeSecurity, "selectedRole");//$NON-NLS-1$ //$NON-NLS-2$
      bf.createBinding(roleListBox, "selectedIndex", addUserToRoleButton, "disabled", buttonConverter);//$NON-NLS-1$ //$NON-NLS-2$
      bf.createBinding(roleDetailTable, "selectedItem", removeUserFromRoleButton, "disabled", removeButtonConverter);//$NON-NLS-1$ //$NON-NLS-2$
      bf.createBinding(userListBox, "selectedIndex", addRoleToUserButton, "disabled", buttonConverter);//$NON-NLS-1$ //$NON-NLS-2$
      bf.createBinding(userDetailTable, "selectedItem", removeRoleFromUserButton, "disabled", removeButtonConverter);//$NON-NLS-1$ //$NON-NLS-2$
      bf.createBinding(eeSecurity, "roleList", roleListBox, "elements").fireSourceChanged();//$NON-NLS-1$ //$NON-NLS-2$
      bf.createBinding(roleListBox, "selectedItem", eeSecurity, "selectedRole");//$NON-NLS-1$ //$NON-NLS-2$

      userDetailBinding = bf.createBinding(eeSecurity, "selectedUser", userDetailTable, "elements",//$NON-NLS-1$ //$NON-NLS-2$
          new BindingConvertor<IUIUser, List<IUIRole>>() {

            @Override
            public List<IUIRole> sourceToTarget(IUIUser ru) {
              if(ru instanceof IUIEEUser) {
                return new ArrayList<IUIRole>(((IUIEEUser)ru).getRoles());                
              } else {
                return null;
              }
            }

            @Override
            public IUIUser targetToSource(List<IUIRole> arg0) {
              // TODO Auto-generated method stub
              return null;
            }

          });
      
      roleDetailBinding = bf.createBinding(eeSecurity, "selectedRole", roleDetailTable, "elements",//$NON-NLS-1$ //$NON-NLS-2$
          new BindingConvertor<IUIRole, List<IUIUser>>() {

            @Override
            public List<IUIUser> sourceToTarget(IUIRole rr) {
              return new ArrayList<IUIUser>(rr.getUsers());
            }

            @Override
            public IUIRole targetToSource(List<IUIUser> arg0) {
              // TODO Auto-generated method stub
              return null;
            }

          });
      bf.createBinding(eeSecurity, "selectedDeck", userRoleDeck, "selectedIndex",//$NON-NLS-1$ //$NON-NLS-2$
          new BindingConvertor<ObjectRecipient.Type, Integer>() {

            @Override
            public Integer sourceToTarget(Type arg0) {
              if (arg0 == Type.ROLE) {
                roleRadioButton.setSelected(true);
                userRadioButton.setSelected(false);
                return 1;
              } else if (arg0 == Type.USER) {
                roleRadioButton.setSelected(false);
                userRadioButton.setSelected(true);
                return 0;
              } else
                return -1;
            }

            @Override
            public Type targetToSource(Integer arg0) {
              return null;
            }

          });
      BindingConvertor<Mode, Boolean> modeBindingConverter = new BindingConvertor<Mode, Boolean>() {

        @Override
        public Boolean sourceToTarget(Mode arg0) {
          if (arg0.equals(Mode.ADD)) {
            return false;
          }
          return true;
        }

        @Override
        public Mode targetToSource(Boolean arg0) {
          // TODO Auto-generated method stub
          return null;
        }

      };
      BindingConvertor<Mode, Boolean> anotherModeBindingConverter = new BindingConvertor<Mode, Boolean>() {

        @Override
        public Boolean sourceToTarget(Mode arg0) {
          if (arg0.equals(Mode.EDIT_MEMBER)) {
            return true;
          } else
            return false;
        }

        @Override
        public Mode targetToSource(Boolean arg0) {
          // TODO Auto-generated method stub
          return null;
        }

      };
      bf.createBinding(securityRole, "mode", rolename, "disabled", modeBindingConverter);//$NON-NLS-1$ //$NON-NLS-2$
      bf.createBinding(securityRole, "mode", roleDescription, "disabled", anotherModeBindingConverter);//$NON-NLS-1$ //$NON-NLS-2$

    } catch (Exception e) {
      // convert to runtime exception so it bubbles up through the UI
      throw new RuntimeException(e);
    }
  }

  public void assignUsersToRole() {
    securityRole.assignUsers(Arrays.asList(availableUsers.getSelectedItems()));
  }

  public void unassignUsersFromRole() {
    securityRole.unassignUsers(Arrays.asList(assignedUsers.getSelectedItems()));
  }

  public void assignRolesToUser() {
    
    eeSecurityUser.assignRoles(Arrays.asList(availableRoles.getSelectedItems()));
  }

  public void unassignRolesFromUser() {
    eeSecurityUser.unassignRoles(Arrays.asList(assignedRoles.getSelectedItems()));
  }
  
  @Override
  public void showAddUserDialog() throws Exception {
    try {
      if (service != null && service.getRoles() != null) {
        eeSecurityUser.clear();
        eeSecurityUser.setAvailableRoles(convertToUIRoleModel(service.getRoles()));
      }
      eeSecurityUser.setMode(Mode.ADD);
      userDialog.setTitle(messages.getString("AddUserDialog.Title"));//$NON-NLS-1$
      userDialog.show();
    } catch (KettleException e) {
      messageBox.setTitle(messages.getString("Dialog.Error"));//$NON-NLS-1$
      messageBox.setAcceptLabel(messages.getString("Dialog.Ok"));//$NON-NLS-1$
      messageBox.setMessage(BaseMessages.getString(RepositoryExplorer.class,
          "SecurityController.AddUser.UnableToShowAddUser", e.getLocalizedMessage()));//$NON-NLS-1$
      messageBox.open();
    }
  }

  public void cancelAddRoleDialog() throws Exception {
    roleDialog.hide();
  }

  /**
   * addRole method is called when user has click ok on a add role dialog. The method add the 
   * role
   * @throws Exception
   */
  @Override
  protected void addUser() throws Exception {
    if (service != null) {
      try {
        service.saveUserInfo(eeSecurityUser.getUserInfo());
        eeSecurity.addUser(UIObjectRegistery.getInstance().constructUIRepositoryUser(eeSecurityUser.getUserInfo()));
      } catch (KettleException ke) {
        messageBox.setTitle(messages.getString("Dialog.Error"));//$NON-NLS-1$
        messageBox.setAcceptLabel(messages.getString("Dialog.Ok"));//$NON-NLS-1$
        messageBox.setMessage(BaseMessages.getString(RepositoryExplorer.class,
            "AddUser.UnableToAddUser", ke.getLocalizedMessage()));//$NON-NLS-1$
        messageBox.open();
      }
    }
    userDialog.hide();
  }
  
  @Override
  public void showEditUserDialog() throws Exception {
    if (service != null && service.getRoles() != null) {
      eeSecurityUser.clear();
      eeSecurityUser.setUser(security.getSelectedUser(), convertToUIRoleModel(service.getRoles()));
      eeSecurityUser.setMode(Mode.EDIT);
      userDialog.setTitle(messages.getString("EditUserDialog.Title"));//$NON-NLS-1$
      userDialog.show();
    }
  }

  /**
   * updateUser method is called when user has click ok on a edit user dialog. The method updates the 
   * user
   * @throws Exception
   */
  @Override
  protected void updateUser() throws Exception {
    if (service != null) {
      try {
        UIEERepositoryUser uiUser = (UIEERepositoryUser) eeSecurity.getSelectedUser();
        Set<IUIRole> previousRoleList = new HashSet<IUIRole>();
        previousRoleList.addAll(uiUser.getRoles());
        uiUser.setDescription(eeSecurityUser.getDescription());
        uiUser.setPassword(eeSecurityUser.getPassword());
        uiUser.setRoles(new HashSet<IUIRole>(eeSecurityUser.getAssignedRoles()));
        service.updateUser(uiUser.getUserInfo());
        eeSecurity.updateUser(uiUser, previousRoleList);
      } catch (KettleException ke) {
        messageBox.setTitle(messages.getString("Dialog.Error"));//$NON-NLS-1$
        messageBox.setAcceptLabel(messages.getString("Dialog.Ok"));//$NON-NLS-1$
        messageBox.setMessage(BaseMessages.getString(RepositoryExplorer.class,
            "UpdateUser.UnableToUpdateUser", ke.getLocalizedMessage()));//$NON-NLS-1$
        messageBox.open();
      }
    }
    userDialog.hide();
  }

  public void showAddRoleDialog() throws Exception {
    try {
      if (service != null && service.getUsers() != null) {
        securityRole.clear();
        securityRole.setAvailableUsers(convertToUIUserModel(service.getUsers()));
      }
      roleDialog.setTitle(messages.getString("AddRoleDialog.Title"));//$NON-NLS-1$
      roleDialog.show();
    } catch (KettleException e) {
      messageBox.setTitle(messages.getString("Dialog.Error"));//$NON-NLS-1$
      messageBox.setAcceptLabel(messages.getString("Dialog.Ok"));//$NON-NLS-1$
      messageBox.setMessage(BaseMessages.getString(RepositoryExplorer.class,
          "SecurityController.AddRole.UnableToShowAddRole", e.getLocalizedMessage()));//$NON-NLS-1$
      messageBox.open();

    }
  }

  public void showAddUserToRoleDialog() throws Exception {
    if (service != null && service.getUsers() != null) {
      securityRole.clear();
      securityRole.setRole(((UIEESecurity) security).getSelectedRole(), convertToUIUserModel(service.getUsers()));
      securityRole.setMode(Mode.EDIT_MEMBER);
    }
    roleDialog.setTitle(messages.getString("AddUserToRoleDialog.Title"));//$NON-NLS-1$
    roleDialog.show();
  }

  public void showAddRoleToUserDialog() throws Exception {
    if (service != null && service.getRoles() != null) {
      eeSecurityUser.clear();
      eeSecurityUser.setUser(security.getSelectedUser(), convertToUIRoleModel(service.getRoles()));
      eeSecurityUser.setMode(Mode.EDIT_MEMBER);
      userDialog.setTitle(messages.getString("AddRoleToUserDialog.Title"));//$NON-NLS-1$
      userDialog.show();
    }
  }

  public void removeRolesFromUser() throws Exception {
    ((UIEESecurity)security).removeRolesFromSelectedUser(userDetailTable.getSelectedItems());
    service.updateUser(security.getSelectedUser().getUserInfo());
  }

  public void removeUsersFromRole() throws Exception {
    ((UIEESecurity)security).removeUsersFromSelectedRole(roleDetailTable.getSelectedItems());
    ((IRoleSupportSecurityManager) service).updateRole(((UIEESecurity)security).getSelectedRole().getRole());
  }

  /**
   * addRole method is called when user has click ok on a add role dialog. The method add the 
   * role
   * @throws Exception
   */

  private void addRole() throws Exception {
    if (service != null) {
      try {
        IRole role = securityRole.getRole(service);
        service.createRole(role);
        eeSecurity.addRole(UIEEObjectRegistery.getInstance().constructUIRepositoryRole(role));
      } catch (KettleException ke) {
        messageBox.setTitle(messages.getString("Dialog.Error"));//$NON-NLS-1$
        messageBox.setAcceptLabel(messages.getString("Dialog.Ok"));//$NON-NLS-1$
        messageBox.setMessage(BaseMessages.getString(RepositoryExplorer.class,
            "AddRole.UnableToAddRole", ke.getLocalizedMessage()));//$NON-NLS-1$
        messageBox.open();
      }
    }
    roleDialog.hide();
  }

  /**
   * updateRole method is called when user has click ok on a edit role dialog. The method updates the 
   * role
   * @throws Exception
   */

  private void updateRole() throws Exception {
    if (service != null) {
      try {
        IUIRole uiRole = eeSecurity.getSelectedRole();
        Set<IUIUser> previousUserList = new HashSet<IUIUser>();
        previousUserList.addAll(uiRole.getUsers());
        uiRole.setDescription(securityRole.getDescription());
        uiRole.setUsers(new HashSet<IUIUser>(securityRole.getAssignedUsers()));
        service.updateRole(uiRole.getRole());
        eeSecurity.updateRole(uiRole, previousUserList);
        roleDialog.hide();
      } catch (KettleException ke) {
        messageBox.setTitle(messages.getString("Dialog.Error"));//$NON-NLS-1$
        messageBox.setAcceptLabel(messages.getString("Dialog.Ok"));//$NON-NLS-1$
        messageBox.setMessage(BaseMessages.getString(RepositoryExplorer.class,
            "UpdateRole.UnableToUpdateRole", ke.getLocalizedMessage()));//$NON-NLS-1$
        messageBox.open();
      }
    }
  }

  /**
   * removeRole method is called when user has click on a remove button in a role deck. It first
   * displays a confirmation message to the user and once the user selects ok, it remove the role
   * @throws Exception
   */

  public void removeRole() throws Exception {
    XulConfirmBox confirmBox = (XulConfirmBox) document.createElement("confirmbox");//$NON-NLS-1$
    confirmBox.setTitle(messages.getString("ConfirmDialog.Title"));//$NON-NLS-1$
    confirmBox.setMessage(messages.getString("RemoveRoleConfirmDialog.Message"));//$NON-NLS-1$
    confirmBox.setAcceptLabel(messages.getString("Dialog.Ok"));//$NON-NLS-1$
    confirmBox.setCancelLabel(messages.getString("Dialog.Cancel"));//$NON-NLS-1$
    confirmBox.addDialogCallback(new XulDialogCallback<Object>() {

      public void onClose(XulComponent sender, Status returnCode, Object retVal) {
        if (returnCode == Status.ACCEPT) {
          if (service != null) {
            if (eeSecurity != null && eeSecurity.getSelectedRole() != null) {
              try {
                service.deleteRole(eeSecurity.getSelectedRole().getName());
                eeSecurity.removeRole(eeSecurity.getSelectedRole().getName());
              } catch (KettleException ke) {
                messageBox.setTitle(messages.getString("Dialog.Error"));//$NON-NLS-1$
                messageBox.setAcceptLabel(messages.getString("Dialog.Ok"));//$NON-NLS-1$
                messageBox.setMessage(BaseMessages.getString(RepositoryExplorer.class,
                    "RemoveRole.UnableToRemoveRole", ke.getLocalizedMessage()));//$NON-NLS-1$
                messageBox.open();
              }
            } else {
              messageBox.setTitle(messages.getString("Dialog.Error"));//$NON-NLS-1$
              messageBox.setAcceptLabel(messages.getString("Dialog.Ok"));//$NON-NLS-1$
              messageBox.setMessage(messages.getString("RemoveRole.NoRoleSelected"));//$NON-NLS-1$
              messageBox.open();
            }
          }
        }
      }

      public void onError(XulComponent sender, Throwable t) {
        messageBox.setTitle(messages.getString("Dialog.Error"));//$NON-NLS-1$
        messageBox.setAcceptLabel(messages.getString("Dialog.Ok"));//$NON-NLS-1$
        messageBox.setMessage(BaseMessages.getString(RepositoryExplorer.class,
            "RemoveRole.UnableToRemoveRole", t.getLocalizedMessage()));//$NON-NLS-1$
        messageBox.open();
      }
    });
    confirmBox.open();
  }

  public void showEditRoleDialog() throws Exception {
    if (service != null && service.getUsers() != null) {
      securityRole.clear();
      securityRole.setRole(eeSecurity.getSelectedRole(), convertToUIUserModel(service.getUsers()));
      securityRole.setMode(Mode.EDIT);
      roleDialog.setTitle(messages.getString("EditRoleDialog.Title"));//$NON-NLS-1$
      roleDialog.show();
    }
  }
  public void changeToRoleDeck() {
    security.setSelectedDeck(ObjectRecipient.Type.ROLE);
  }
  /**
   * saveRole method is called when the user click on the ok button of a Add or Edit Role dialog
   * Depending on the mode it calls add of update role method
   * @throws Exception
   */
  public void saveRole() throws Exception {
    if (securityRole.getMode().equals(Mode.ADD)) {
      addRole();
    } else {
      updateRole();
    }
  }

  private List<IUIRole> convertToUIRoleModel(List<IRole> roles) throws UIObjectCreationException {
    List<IUIRole> rroles = new ArrayList<IUIRole>();
    for (IRole role : roles) {
      rroles.add(UIEEObjectRegistery.getInstance().constructUIRepositoryRole(role));
    }
    return rroles;
  }

  private List<IUIUser> convertToUIUserModel(List<IUser> users) throws UIObjectCreationException {
    List<IUIUser> rusers = new ArrayList<IUIUser>();
    for (IUser user : users) {
      rusers.add(UIObjectRegistery.getInstance().constructUIRepositoryUser(user));
    }
    return rusers;
  }  
}
