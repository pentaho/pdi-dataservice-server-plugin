/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.ui.repository.pur.repositoryexplorer.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.List;
import java.util.ResourceBundle;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositorySecurityManager;
import org.pentaho.di.repository.RepositorySecurityProvider;
import org.pentaho.di.repository.pur.model.ObjectAcl;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.IAclObject;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.ILockObject;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.IUIEEUser;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.model.UIRepositoryObjectAcl;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.model.UIRepositoryObjectAclModel;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.model.UIRepositoryObjectAcls;
import org.pentaho.di.ui.repository.repositoryexplorer.AccessDeniedException;
import org.pentaho.di.ui.repository.repositoryexplorer.ContextChangeVetoer;
import org.pentaho.di.ui.repository.repositoryexplorer.ControllerInitializationException;
import org.pentaho.di.ui.repository.repositoryexplorer.IUISupportController;
import org.pentaho.di.ui.repository.repositoryexplorer.controllers.IBrowseController;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryContent;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryDirectory;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryObject;
import org.pentaho.platform.api.repository2.unified.RepositoryFilePermission;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingConvertor;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.binding.DefaultBindingFactory;
import org.pentaho.ui.xul.components.XulButton;
import org.pentaho.ui.xul.components.XulCheckbox;
import org.pentaho.ui.xul.components.XulConfirmBox;
import org.pentaho.ui.xul.components.XulLabel;
import org.pentaho.ui.xul.components.XulMessageBox;
import org.pentaho.ui.xul.containers.XulDeck;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.containers.XulListbox;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;
import org.pentaho.ui.xul.util.XulDialogCallback;

/**
 *
 * This is the XulEventHandler for the browse panel of the repository explorer. It sets up the bindings for
 * browse functionality.
 *
 */
public class PermissionsController extends AbstractXulEventHandler implements ContextChangeVetoer, IUISupportController, java.io.Serializable {

  private static final long serialVersionUID = -6151060931568671109L; /* EESOURCE: UPDATE SERIALVERUID */

  private static final Class<?> PKG = IUIEEUser.class;

  public static final String NEWLINE = "\n"; //$NON-NLS-1$

  protected ResourceBundle messages = new ResourceBundle() {

    @Override
    public Enumeration<String> getKeys() {
      return null;
    }

    @Override
    protected Object handleGetObject(String key) {
      return BaseMessages.getString(PKG, key);
    }

  };

  private static final int NO_ACL = 0;

  private static final int ACL = 1;

  private XulDeck aclDeck;

  private XulListbox userRoleList;

  private XulListbox availableUserList;

  private XulListbox availableRoleList;

  private XulListbox selectedUserList;

  private XulListbox selectedRoleList;

  private XulCheckbox writeCheckbox;

  private XulCheckbox readCheckbox;

  private XulCheckbox inheritParentPermissionCheckbox;

  private XulButton addAclButton;

  private XulButton removeAclButton;

  private XulCheckbox manageAclCheckbox;
  
  private XulCheckbox deleteCheckbox;

  private XulDialog manageAclsDialog;

  //private XulDialog applyAclConfirmationDialog;

  private XulButton assignUserButton;

  private XulButton unassignUserButton;

  private XulButton assignRoleButton;

  private XulButton unassignRoleButton;

  //private XulRadio applyOnlyRadioButton;

  //private XulRadio applyRecursiveRadioButton;

  private Binding securityBinding;

  private XulLabel fileFolderLabel;

  BindingFactory bf;

  private UIRepositoryObjectAcls viewAclsModel;

  UIRepositoryObjectAclModel manageAclsModel = null;

  XulConfirmBox confirmBox = null;

  XulMessageBox messageBox = null;

  List<UIRepositoryObject> repoObject = new ArrayList<UIRepositoryObject>();

  private RepositorySecurityProvider service;

  private IBrowseController browseController;

  ObjectAcl acl;

  TYPE returnType;

  public PermissionsController() {
  }

  public void init(Repository rep) throws ControllerInitializationException {
    try {
      browseController = (IBrowseController) this.getXulDomContainer().getEventHandler("browseController");
      if (rep != null && rep.hasService(RepositorySecurityProvider.class)) {
        service = (RepositorySecurityProvider) rep.getService(RepositorySecurityProvider.class);
      } else {
        throw new ControllerInitializationException(BaseMessages.getString(PKG,
            "PermissionsController.ERROR_0001_UNABLE_TO_INITIAL_REPOSITORY_SERVICE", RepositorySecurityManager.class)); //$NON-NLS-1$

      }

      confirmBox = (XulConfirmBox) document.createElement("confirmbox");//$NON-NLS-1$
      confirmBox.setTitle(BaseMessages.getString(PKG, "PermissionsController.RemoveAclWarning")); //$NON-NLS-1$
      confirmBox.setMessage(BaseMessages.getString(PKG, "PermissionsController.RemoveAclWarningText")); //$NON-NLS-1$
      confirmBox.setAcceptLabel(BaseMessages.getString(PKG, "Dialog.Ok")); //$NON-NLS-1$
      confirmBox.setCancelLabel(BaseMessages.getString(PKG, "Dialog.Cancel")); //$NON-NLS-1$
      confirmBox.addDialogCallback(new XulDialogCallback<Object>() {

        public void onClose(XulComponent sender, Status returnCode, Object retVal) {
          if (returnCode == Status.ACCEPT) {
            viewAclsModel.removeSelectedAcls();
          }
        }

        public void onError(XulComponent sender, Throwable t) {

        }
      });

      messageBox = (XulMessageBox) document.createElement("messagebox");//$NON-NLS-1$ 
      viewAclsModel = new UIRepositoryObjectAcls();
      manageAclsModel = new UIRepositoryObjectAclModel(viewAclsModel);
      browseController.addContextChangeVetoer(this);
      bf = new DefaultBindingFactory();
      bf.setDocument(this.getXulDomContainer().getDocumentRoot());
      createBindings();
    } catch (Exception e) {
      throw new ControllerInitializationException(e);
    }

  }

  private void createBindings() {
    fileFolderLabel = (XulLabel) document.getElementById("file-folder-name");//$NON-NLS-1$ 
    aclDeck = (XulDeck) document.getElementById("acl-deck");//$NON-NLS-1$ 
    // Permission Tab Binding
    userRoleList = (XulListbox) document.getElementById("user-role-list");//$NON-NLS-1$ 
    writeCheckbox = (XulCheckbox) document.getElementById("write-checkbox");//$NON-NLS-1$ 
    readCheckbox = (XulCheckbox) document.getElementById("read-checkbox");//$NON-NLS-1$ 

    inheritParentPermissionCheckbox = (XulCheckbox) document.getElementById("inherit-from-parent-permission-checkbox");//$NON-NLS-1$ 
    manageAclCheckbox = (XulCheckbox) document.getElementById("manage-checkbox");//$NON-NLS-1$ 
    deleteCheckbox = (XulCheckbox) document.getElementById("delete-checkbox");//$NON-NLS-1$
    manageAclsDialog = (XulDialog) document.getElementById("manage-acls-dialog");//$NON-NLS-1$ 
    addAclButton = (XulButton) document.getElementById("add-acl-button");//$NON-NLS-1$ 
    removeAclButton = (XulButton) document.getElementById("remove-acl-button");//$NON-NLS-1$ 

    // Add/Remove Acl Binding
    availableUserList = (XulListbox) document.getElementById("available-user-list");//$NON-NLS-1$ 
    selectedUserList = (XulListbox) document.getElementById("selected-user-list");//$NON-NLS-1$ 
    availableRoleList = (XulListbox) document.getElementById("available-role-list");//$NON-NLS-1$ 
    selectedRoleList = (XulListbox) document.getElementById("selected-role-list");//$NON-NLS-1$ 

    assignRoleButton = (XulButton) document.getElementById("assign-role");//$NON-NLS-1$ 
    unassignRoleButton = (XulButton) document.getElementById("unassign-role");//$NON-NLS-1$ 
    assignUserButton = (XulButton) document.getElementById("assign-user");//$NON-NLS-1$ 
    unassignUserButton = (XulButton) document.getElementById("unassign-user");//$NON-NLS-1$ 

    //applyAclConfirmationDialog = (XulDialog) document.getElementById("apply-acl-confirmation-dialog");//$NON-NLS-1$ 
    //applyOnlyRadioButton = (XulRadio) document.getElementById("apply-only-radio-button");//$NON-NLS-1$ 
    //applyRecursiveRadioButton = (XulRadio) document.getElementById("apply-recursive-radio-button");//$NON-NLS-1$ 

    // Binding the model user or role list to the ui user or role list
    bf.setBindingType(Binding.Type.ONE_WAY);
    bf.createBinding(manageAclsModel, "availableUserList", availableUserList, "elements");//$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(manageAclsModel, "selectedUserList", selectedUserList, "elements");//$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(manageAclsModel, "availableRoleList", availableRoleList, "elements"); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(manageAclsModel, "selectedRoleList", selectedRoleList, "elements"); //$NON-NLS-1$ //$NON-NLS-2$

    // indicesToObjectsConverter convert the selected indices to the list of objects and vice versa
    BindingConvertor<int[], List<UIRepositoryObjectAcl>> indicesToObjectsConverter = new BindingConvertor<int[], List<UIRepositoryObjectAcl>>() {

      @Override
      public int[] targetToSource(List<UIRepositoryObjectAcl> acls) {
        if (acls != null) {
          int i = 0;
          int[] retVal = new int[acls.size()];
          for (UIRepositoryObjectAcl acl : acls) {
            retVal[i++] = viewAclsModel.getAceIndex(acl.getAce());
          }
          return retVal;
        }
        return null;
      }

      @Override
      public List<UIRepositoryObjectAcl> sourceToTarget(int[] indices) {
        if (indices != null && indices.length > 0) {
          List<UIRepositoryObjectAcl> retVal = new ArrayList<UIRepositoryObjectAcl>();
          for (int i = 0; i < indices.length; i++) {
            retVal.add(new UIRepositoryObjectAcl(viewAclsModel.getAceAtIndex(indices[i])));
          }
          return retVal;
        }
        return null;
      }

    };

    // indexToAvalableUserConverter convert the selected indices to the list of objects and vice versa
    BindingConvertor<int[], List<String>> indexToAvailableUserConverter = new BindingConvertor<int[], List<String>>() {

      @Override
      public List<String> sourceToTarget(int[] indices) {
        List<String> userList = new ArrayList<String>();
        for (int i = 0; i < indices.length; i++) {
          userList.add(manageAclsModel.getAvailableUser(indices[i]));
        }
        return userList;
      }

      @Override
      public int[] targetToSource(List<String> userList) {
        int[] indices = new int[userList.size()];
        int i = 0;
        for (String user : userList) {
          indices[i++] = manageAclsModel.getAvailableUserIndex(user);
        }
        return indices;
      }

    };

    BindingConvertor<int[], List<String>> indexToAvailableRoleConverter = new BindingConvertor<int[], List<String>>() {

      @Override
      public List<String> sourceToTarget(int[] indices) {
        List<String> roleList = new ArrayList<String>();
        for (int i = 0; i < indices.length; i++) {
          roleList.add(manageAclsModel.getAvailableRole(indices[i]));
        }
        return roleList;
      }

      @Override
      public int[] targetToSource(List<String> roleList) {
        int[] indices = new int[roleList.size()];
        int i = 0;
        for (String role : roleList) {
          indices[i++] = manageAclsModel.getAvailableRoleIndex(role);
        }
        return indices;
      }
    };

    BindingConvertor<int[], List<UIRepositoryObjectAcl>> indexToSelectedUserConverter = new BindingConvertor<int[], List<UIRepositoryObjectAcl>>() {

      @Override
      public List<UIRepositoryObjectAcl> sourceToTarget(int[] indices) {
        List<UIRepositoryObjectAcl> userList = new ArrayList<UIRepositoryObjectAcl>();
        for (int i = 0; i < indices.length; i++) {
          userList.add(manageAclsModel.getSelectedUser(indices[i]));
        }
        return userList;
      }

      @Override
      public int[] targetToSource(List<UIRepositoryObjectAcl> userList) {
        int[] indices = new int[userList.size()];
        int i = 0;
        for (UIRepositoryObjectAcl user : userList) {
          indices[i++] = manageAclsModel.getSelectedUserIndex(user);
        }
        return indices;
      }

    };

    BindingConvertor<int[], List<UIRepositoryObjectAcl>> indexToSelectedRoleConverter = new BindingConvertor<int[], List<UIRepositoryObjectAcl>>() {

      @Override
      public List<UIRepositoryObjectAcl> sourceToTarget(int[] indices) {
        List<UIRepositoryObjectAcl> roleList = new ArrayList<UIRepositoryObjectAcl>();
        for (int i = 0; i < indices.length; i++) {
          roleList.add(manageAclsModel.getSelectedRole(indices[i]));
        }
        return roleList;
      }

      @Override
      public int[] targetToSource(List<UIRepositoryObjectAcl> roleList) {
        int[] indices = new int[roleList.size()];
        int i = 0;
        for (UIRepositoryObjectAcl role : roleList) {
          indices[i++] = manageAclsModel.getSelectedRoleIndex(role);
        }
        return indices;
      }
    };

    // Binding between the selected incides of the lists to the mode list objects
    bf.setBindingType(Binding.Type.BI_DIRECTIONAL);

    bf.createBinding(availableUserList, "selectedIndices", manageAclsModel, "selectedAvailableUsers",//$NON-NLS-1$ //$NON-NLS-2$
        indexToAvailableUserConverter);
    bf.createBinding(selectedUserList, "selectedIndices", manageAclsModel, "selectedAssignedUsers",//$NON-NLS-1$ //$NON-NLS-2$
        indexToSelectedUserConverter);
    bf.createBinding(availableRoleList, "selectedIndices", manageAclsModel, "selectedAvailableRoles",//$NON-NLS-1$ //$NON-NLS-2$
        indexToAvailableRoleConverter);
    bf.createBinding(selectedRoleList, "selectedIndices", manageAclsModel, "selectedAssignedRoles",//$NON-NLS-1$ //$NON-NLS-2$
        indexToSelectedRoleConverter);

    // accumulatorButtonConverter determine whether to enable of disable the accumulator buttons
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
        // TODO Auto-generated method stub
        return null;
      }
    };
    bf.setBindingType(Binding.Type.ONE_WAY);
    bf.createBinding(selectedUserList, "selectedIndex", manageAclsModel, "userUnassignmentPossible",//$NON-NLS-1$ //$NON-NLS-2$
        accumulatorButtonConverter);
    bf.createBinding(availableUserList, "selectedIndex", manageAclsModel, "userAssignmentPossible",//$NON-NLS-1$ //$NON-NLS-2$
        accumulatorButtonConverter);
    bf.createBinding(manageAclsModel, "userUnassignmentPossible", unassignUserButton, "!disabled");//$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(manageAclsModel, "userAssignmentPossible", assignUserButton, "!disabled");//$NON-NLS-1$ //$NON-NLS-2$

    bf.createBinding(selectedRoleList, "selectedIndex", manageAclsModel, "roleUnassignmentPossible",//$NON-NLS-1$ //$NON-NLS-2$
        accumulatorButtonConverter);
    bf.createBinding(availableRoleList, "selectedIndex", manageAclsModel, "roleAssignmentPossible",//$NON-NLS-1$ //$NON-NLS-2$
        accumulatorButtonConverter);

    bf.createBinding(manageAclsModel, "roleUnassignmentPossible", unassignRoleButton, "!disabled");//$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(manageAclsModel, "roleAssignmentPossible", assignRoleButton, "!disabled");//$NON-NLS-1$ //$NON-NLS-2$

    bf.setBindingType(Binding.Type.ONE_WAY);

    BindingConvertor<List<UIRepositoryObject>, List<UIRepositoryObjectAcl>> securityBindingConverter = new BindingConvertor<List<UIRepositoryObject>, List<UIRepositoryObjectAcl>>() {
      @Override
      public List<UIRepositoryObjectAcl> sourceToTarget(List<UIRepositoryObject> ro) {
        if (ro == null) {
          return null;
        }
        if (ro.size() <= 0) {
          return null;
        }
        setSelectedRepositoryObject(ro);
        viewAclsModel.setRemoveEnabled(false);
        List<UIRepositoryObjectAcl> selectedAclList = Collections.emptyList();
        // we've moved to a new file/folder; need to clear out what the model thinks is selected
        viewAclsModel.setSelectedAclList(selectedAclList);
        uncheckAllPermissionBox();
        UIRepositoryObject repoObject = (UIRepositoryObject) ro.get(0);
        try {
          if(repoObject instanceof IAclObject) {
            ((IAclObject) repoObject).getAcls(viewAclsModel);  
          } else {
            throw new IllegalStateException(BaseMessages.getString(PKG, "PermissionsController.NoAclSupport")); //$NON-NLS-1$
          }          
          
          fileFolderLabel.setValue(BaseMessages.getString(PKG,
              "AclTab.UserRolePermission", repoObject.getName())); //$NON-NLS-1$
          bf.setBindingType(Binding.Type.ONE_WAY);
          bf.createBinding(viewAclsModel, "acls", userRoleList, "elements"); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (AccessDeniedException ade) {
          messageBox.setTitle(BaseMessages.getString(PKG, "Dialog.Error"));//$NON-NLS-1$
          messageBox.setAcceptLabel(BaseMessages.getString(PKG, "Dialog.Ok"));//$NON-NLS-1$
          messageBox.setMessage(BaseMessages.getString(PKG,
              "PermissionsController.UnableToGetAcls", repoObject.getName(), ade.getLocalizedMessage()));//$NON-NLS-1$

          messageBox.open();
        } catch (Exception e) {
          messageBox.setTitle(BaseMessages.getString(PKG, "Dialog.Error"));//$NON-NLS-1$
          messageBox.setAcceptLabel(BaseMessages.getString(PKG, "Dialog.Ok"));//$NON-NLS-1$
          messageBox.setMessage(BaseMessages.getString(PKG,
              "PermissionsController.UnableToGetAcls", repoObject.getName(), e.getLocalizedMessage())); //$NON-NLS-1$
          messageBox.open();
        }

        aclDeck.setSelectedIndex(ACL);
        return viewAclsModel.getAcls();
      }

      @Override
      public List<UIRepositoryObject> targetToSource(List<UIRepositoryObjectAcl> elements) {
        return null;
      }
    };

    // Binding between the selected repository objects and the user role list for acls
    securityBinding = bf.createBinding(browseController,
        "repositoryObjects", userRoleList, "elements", securityBindingConverter);//$NON-NLS-1$ //$NON-NLS-2$
    securityBinding = bf.createBinding(browseController,
        "repositoryDirectories", userRoleList, "elements", securityBindingConverter);//$NON-NLS-1$ //$NON-NLS-2$

    bf.setBindingType(Binding.Type.BI_DIRECTIONAL);

    // Binding Add Remove button to the inherit check box. If the checkbox is checked that disable add remove
    bf.createBinding(viewAclsModel, "entriesInheriting", inheritParentPermissionCheckbox, "checked"); //$NON-NLS-1$  //$NON-NLS-2$
    // Binding the selected indices of acl list to the list of acl objects in the mode
    bf.createBinding(userRoleList, "selectedIndices", viewAclsModel, "selectedAclList", indicesToObjectsConverter); //$NON-NLS-1$  //$NON-NLS-2$

    bf.setBindingType(Binding.Type.ONE_WAY);
    // Only enable add Acl button if the entries checkbox is unchecked
    bf.createBinding(viewAclsModel, "entriesInheriting", addAclButton, "disabled");//$NON-NLS-1$  //$NON-NLS-2$ 
    // Only enable remove Acl button if the entries checkbox is unchecked and acl is selected from the list
    bf.createBinding(viewAclsModel, "removeEnabled", removeAclButton, "!disabled"); //$NON-NLS-1$  //$NON-NLS-2$ 
    bf.createBinding(viewAclsModel, "removeEnabled", writeCheckbox, "!disabled"); //$NON-NLS-1$  //$NON-NLS-2$
    //bf.createBinding(viewAclsModel, "removeEnabled", readCheckbox, "!disabled");//$NON-NLS-1$  //$NON-NLS-2$
    bf.createBinding(viewAclsModel, "removeEnabled", manageAclCheckbox, "!disabled");//$NON-NLS-1$  //$NON-NLS-2$
    bf.createBinding(viewAclsModel, "removeEnabled", deleteCheckbox, "!disabled");//$NON-NLS-1$  //$NON-NLS-2$
    //bf.createBinding(viewAclsModel, "removeEnabled", viewCheckbox, "!disabled");//$NON-NLS-1$  //$NON-NLS-2$
    bf.setBindingType(Binding.Type.ONE_WAY);
    // Binding when the user select from the list

    bf.createBinding(viewAclsModel, "selectedAclList", this, "aclState", //$NON-NLS-1$  //$NON-NLS-2$
        new BindingConvertor<List<UIRepositoryObjectAcl>, UIRepositoryObjectAcl>() {

          @Override
          public UIRepositoryObjectAcl sourceToTarget(List<UIRepositoryObjectAcl> value) {
            if (value != null && value.size() > 0) {
              return value.get(0);
            }
            return null;
          }

          @Override
          public List<UIRepositoryObjectAcl> targetToSource(UIRepositoryObjectAcl value) {
            return null;
          }
        });
    bf.createBinding(userRoleList, "selectedItem", this, "recipientChanged"); //$NON-NLS-1$  //$NON-NLS-2$
    // Setting the default Deck to show no permission
    aclDeck.setSelectedIndex(NO_ACL);
    try {
      if (securityBinding != null) {
        securityBinding.fireSourceChanged();
      }
    } catch (Exception e) {
      // convert to runtime exception so it bubbles up through the UI
      throw new RuntimeException(e);
    }
  }

  public void setSelectedRepositoryObject(List<UIRepositoryObject> roList) {
    if (roList != null) {
      repoObject.clear();
      repoObject.addAll(roList);
    }
  }

  public List<UIRepositoryObject> getSelectedRepositoryObject() {
    return repoObject;
  }

  public String getName() {
    return "permissionsController";//$NON-NLS-1$
  }

  /**
   * 
   * assignUsers method is call to add  selected user(s) to the assign users list
   */
  public void assignUsers() {
    manageAclsModel.assignUsers(Arrays.asList(availableUserList.getSelectedItems()));
  }

  /**
   * unassignUsers method is call to add  unselected user(s) from the assign users list
   */
  public void unassignUsers() {
    manageAclsModel.unassign(Arrays.asList(selectedUserList.getSelectedItems()));
  }

  /**
   * assignRoles method is call to add  selected role(s) to the assign roles list
   */
  public void assignRoles() {
    manageAclsModel.assignRoles(Arrays.asList(availableRoleList.getSelectedItems()));
  }

  /**
   * unassignRoles method is call to add  unselected role(s) from the assign roles list
   */
  public void unassignRoles() {
    manageAclsModel.unassign(Arrays.asList(selectedRoleList.getSelectedItems()));
  }

  public void showManageAclsDialog() throws Exception {
    try {
      manageAclsModel.clear();
      manageAclsModel.setAclsList(service.getAllUsers(), service.getAllRoles());
    } catch (KettleException ke) {
      messageBox.setTitle(BaseMessages.getString(PKG, "Dialog.Error")); //$NON-NLS-1$
      messageBox.setAcceptLabel(BaseMessages.getString(PKG, "Dialog.Ok")); //$NON-NLS-1$
      messageBox.setMessage(BaseMessages.getString(PKG,
          "PermissionsController.UnableToGetUserOrRole", ke.getLocalizedMessage()));//$NON-NLS-1$

      messageBox.open();
    }
    manageAclsDialog.show();
  }

  public void closeManageAclsDialog() throws Exception {
    manageAclsDialog.hide();
  }

  /**
   * updateAcls method is called when the user click ok on the manage acl dialog. It updates the selection to
   * the model
   * @throws Exception
   */
  public void updateAcls() throws Exception {
    manageAclsModel.updateSelectedAcls();
    closeManageAclsDialog();
  }

  /**
   * removeAcl method is called when the user select a or a list of acls to remove from the list.
   * It first display a confirmation box to the user asking to confirm the removal. If the user
   * selected ok, it deletes selected acls from the list
   * @throws Exception
   */
  public void removeAcl() throws Exception {
    confirmBox.open();
  }

  /**
   * apply method is called when the user clicks the apply button on the UI
   */
  public void apply() {
    List<UIRepositoryObject> roList = getSelectedRepositoryObject();
    /*if (roList != null && roList.size() == 1 && (roList.get(0) instanceof UIRepositoryDirectory)) {
      applyAclConfirmationDialog.show();
    } else {*/
      applyOnObjectOnly(roList, false);
    /*}*/

  }

  /**
   * applyOnObjectOnly is called to save acl for a file object only
   * @param roList
   * @param hideDialog
   */
  private void applyOnObjectOnly(List<UIRepositoryObject> roList, boolean hideDialog) {
    try {
      if (roList.get(0) instanceof UIRepositoryDirectory) {
        UIRepositoryDirectory rd = (UIRepositoryDirectory) roList.get(0);
        if (rd instanceof IAclObject) {
          ((IAclObject) rd).setAcls(viewAclsModel);
        } else {
          throw new IllegalStateException(BaseMessages.getString(PKG, "PermissionsController.NoAclSupport")); //$NON-NLS-1$
        }

      } else {
        UIRepositoryContent rc = (UIRepositoryContent) roList.get(0);
        if (rc instanceof ILockObject
            && ((ILockObject)rc).isLocked()) {
            messageBox.setTitle(BaseMessages.getString(PKG, "Dialog.Error"));//$NON-NLS-1$
            messageBox.setAcceptLabel(BaseMessages.getString(PKG, "Dialog.Ok"));//$NON-NLS-1$
            messageBox.setMessage(BaseMessages.getString(PKG, "PermissionsController.LockedObjectWarning")); //$NON-NLS-1$
            messageBox.open();
            viewAclsModel.setModelDirty(false);
            return;
        } else if (rc instanceof IAclObject) {
          ((IAclObject) rc).setAcls(viewAclsModel);
        } else {
          throw new IllegalStateException(BaseMessages.getString(PKG, "PermissionsController.NoAclSupport")); //$NON-NLS-1$
        }
      }
      /*if (hideDialog) {
        applyAclConfirmationDialog.hide();
      }*/
      viewAclsModel.setModelDirty(false);
      messageBox.setTitle(BaseMessages.getString(PKG, "Dialog.Success")); //$NON-NLS-1$
      messageBox.setAcceptLabel(BaseMessages.getString(PKG, "Dialog.Ok")); //$NON-NLS-1$
      messageBox.setMessage(BaseMessages.getString(PKG, "PermissionsController.PermissionAppliedSuccessfully")); //$NON-NLS-1$
      messageBox.open();
    } catch (AccessDeniedException ade) {
      /*if (hideDialog) {
        applyAclConfirmationDialog.hide();
      }*/
      messageBox.setTitle(BaseMessages.getString(PKG, "Dialog.Error")); //$NON-NLS-1$
      messageBox.setAcceptLabel(BaseMessages.getString(PKG, "Dialog.Ok")); //$NON-NLS-1$
      messageBox.setMessage(ade.getLocalizedMessage());
      messageBox.open();
    } catch (KettleException kex) {
        messageBox.setTitle(BaseMessages.getString(PKG, "Dialog.Error")); //$NON-NLS-1$
        messageBox.setAcceptLabel(BaseMessages.getString(PKG, "Dialog.Ok")); //$NON-NLS-1$
        messageBox.setMessage(kex.getLocalizedMessage());
        messageBox.open();
    }
  }

  /* TODO Once we have the functionality to apply permission recursively to the folder and its children
   * we need to uncomment the section below 
   *
  public void setApplyOnly() {
    applyOnlyRadioButton.setSelected(true);
    applyRecursiveRadioButton.setSelected(false);
  }

  public void setApplyRecursive() {
    applyOnlyRadioButton.setSelected(false);
    applyRecursiveRadioButton.setSelected(true);
  }
*/
  /**
   * applyAcl is called to save the acls back to the repository
   * @throws Exception
   */
  public void applyAcl() throws Exception {
    /* TODO Once we have the functionality to apply permission recursively to the folder and its children
     * we need to uncomment the section below 
     *
    // We will call the the server apply method that only applies this acls changes on the current object
    /*if (applyOnlyRadioButton.isSelected()) {*/
      List<UIRepositoryObject> roList = getSelectedRepositoryObject();
      applyOnObjectOnly(roList, true);
    /*} else {
      // TODO We will call the the server apply method that applies this acls changes on the current object and its children
      applyAclConfirmationDialog.hide();
      messageBox.setTitle(BaseMessages.getString(PKG, "Dialog.Error")); //$NON-NLS-1$
      messageBox.setAcceptLabel(BaseMessages.getString(PKG, "Dialog.Ok")); //$NON-NLS-1$
      messageBox.setMessage(BaseMessages.getString(PKG, "PermissionsController.Error.FunctionalityNotSupported")); //$NON-NLS-1$
      messageBox.open();
    }*/
  }

  /*public void closeApplyAclConfirmationDialog() {
    applyAclConfirmationDialog.hide();
  }*/

  /*
   * The method is called when a user select an acl from the acl list. This method reads the current selected
   * acl and populates the UI with the details
   */
  public void setRecipientChanged(UIRepositoryObjectAcl acl) throws Exception {
    List<UIRepositoryObjectAcl> acls = new ArrayList<UIRepositoryObjectAcl>();
    // acl == null when user deselects recipient (CTRL-click)
    if (acl != null) {
      acls.add(acl);
    }
    viewAclsModel.setSelectedAclList(acls);
  }

  public void setAclState(UIRepositoryObjectAcl acl) {
    uncheckAllPermissionBox();
    if (acl != null && acl.getPermissionSet() != null) {
      for (RepositoryFilePermission permission : acl.getPermissionSet()) {
        if (permission.equals(RepositoryFilePermission.ALL)) {
          checkAllPermissionBox();
          break;
        } else if (permission.equals(RepositoryFilePermission.READ)) {
          readCheckbox.setChecked(true);
        } else if (permission.equals(RepositoryFilePermission.WRITE)) {
          writeCheckbox.setChecked(true);
        } else if (permission.equals(RepositoryFilePermission.ACL_MANAGEMENT)) {
          manageAclCheckbox.setChecked(true);
        } else if (permission.equals(RepositoryFilePermission.DELETE)) {
          deleteCheckbox.setChecked(true);
        }
      }
    } else {
      uncheckAllPermissionBox();
    }
  }

  private void uncheckAllPermissionBox() {
    readCheckbox.setChecked(false);
    writeCheckbox.setChecked(false);
    manageAclCheckbox.setChecked(false);
    deleteCheckbox.setChecked(false);
  }

  private void checkAllPermissionBox() {
    readCheckbox.setChecked(true);
    writeCheckbox.setChecked(true);
    manageAclCheckbox.setChecked(true);
    deleteCheckbox.setChecked(true);
  }

  /*
   * updatePermission method is called when the user checks or uncheck any permission checkbox.
   * This method updates the current model with the update value from the UI
   */
  public void updatePermission() {
    UIRepositoryObjectAcl acl = (UIRepositoryObjectAcl) userRoleList.getSelectedItem();
    if (acl == null) {
      throw new IllegalStateException(BaseMessages.getString(PKG, "PermissionsController.NoSelectedRecipient"));
    }
    EnumSet<RepositoryFilePermission> permissions = acl.getPermissionSet();
    if (permissions == null) {
      permissions = EnumSet.noneOf(RepositoryFilePermission.class);
    } else {
      permissions.remove(RepositoryFilePermission.ALL);
    }
    if (readCheckbox.isChecked()) {
      permissions.add(RepositoryFilePermission.READ);
    } else {
      permissions.remove(RepositoryFilePermission.READ);
    }
    if (writeCheckbox.isChecked()) {
      permissions.add(RepositoryFilePermission.WRITE);
    } else {
      permissions.remove(RepositoryFilePermission.WRITE);
    }
    if (manageAclCheckbox.isChecked()) {
      permissions.add(RepositoryFilePermission.ACL_MANAGEMENT);
    } else {
      permissions.remove(RepositoryFilePermission.ACL_MANAGEMENT);
    }
    if (deleteCheckbox.isChecked()) {
      permissions.add(RepositoryFilePermission.DELETE);
    } else {
      permissions.remove(RepositoryFilePermission.DELETE);
    }
    acl.setPermissionSet(permissions);
    viewAclsModel.updateAcl(acl);
  }

  /*
   * If the user check or unchecks the inherit from parent checkbox, this method is called.
   */
  public void updateInheritFromParentPermission() throws Exception {
    // viewAclsModel.clear();
    viewAclsModel.setEntriesInheriting(inheritParentPermissionCheckbox.isChecked());
    if (inheritParentPermissionCheckbox.isChecked()) {
      UIRepositoryObject ro = repoObject.get(0);
      if (ro instanceof IAclObject) {
        // force inherit to true to get effective ACLs before apply...
        ((IAclObject) ro).clearAcl();
        ((IAclObject) ro).getAcls(viewAclsModel, true);
      }
    }

    /*
    if (inheritParentPermissionCheckbox.isChecked()) {
      uncheckAllPermissionBox();
    }
    */
  }

  /*
   * (non-Javadoc)
   * @see org.pentaho.di.ui.repository.repositoryexplorer.ContextChangeListener#onContextChange()
   * This method is called whenever user change the folder or file selection
   */
  public TYPE onContextChange() {
    if (viewAclsModel.isModelDirty()) {
      confirmBox.setTitle(BaseMessages.getString(PKG, "PermissionsController.ContextChangeWarning")); //$NON-NLS-1$
      confirmBox.setMessage(BaseMessages.getString(PKG, "PermissionsController.ContextChangeWarningText")); //$NON-NLS-1$
      confirmBox.setAcceptLabel(BaseMessages.getString(PKG, "Dialog.Yes")); //$NON-NLS-1$
      confirmBox.setCancelLabel(BaseMessages.getString(PKG, "Dialog.No")); //$NON-NLS-1$
      confirmBox.addDialogCallback(new XulDialogCallback<Object>() {

        public void onClose(XulComponent sender, Status returnCode, Object retVal) {
          if (returnCode == Status.ACCEPT) {
            returnType = TYPE.OK;
            viewAclsModel.clear();
            // Clear the ACL from the backing repo object
            UIRepositoryObject ro = (UIRepositoryObject) repoObject.get(0);
            if (ro instanceof IAclObject) {
              ((IAclObject) ro).clearAcl();
            }
            viewAclsModel.setModelDirty(false);
          } else {
            returnType = TYPE.CANCEL;
          }
        }

        public void onError(XulComponent sender, Throwable t) {
          returnType = TYPE.NO_OP;
        }
      });
      confirmBox.open();
    } else {
      returnType = TYPE.NO_OP;
    }
    return returnType;
  }
}
