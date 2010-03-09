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
 * Copyright (c) 2009 Pentaho Corporation..  All rights reserved.
 */
package org.pentaho.di.ui.repository.repositoryexplorer.abs;

import java.util.Enumeration;
import java.util.ResourceBundle;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.IAbsSecurityManager;
import org.pentaho.di.repository.IAbsSecurityProvider;
import org.pentaho.di.repository.IRoleSupportSecurityManager;
import org.pentaho.di.repository.pur.PluginLicenseVerifier;
import org.pentaho.di.ui.repository.ManageRolesUISupport;
import org.pentaho.di.ui.repository.repositoryexplorer.UIEEObjectRegistery;
import org.pentaho.di.ui.repository.repositoryexplorer.UISupportRegistery;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.controller.ChangedWarningController;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.controller.RepositoryExplorerController;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.model.UIAbsRepositoryRole;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIEERepositoryUser;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIObjectRegistery;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.SpoonLifecycleListener;
import org.pentaho.di.ui.spoon.SpoonPerspective;
import org.pentaho.di.ui.spoon.SpoonPlugin;
import org.pentaho.di.ui.spoon.SpoonPluginCategories;
import org.pentaho.di.ui.spoon.SpoonPluginInterface;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.components.XulMenuitem;
import org.pentaho.ui.xul.components.XulMessageBox;
import org.pentaho.ui.xul.components.XulToolbarbutton;
import org.pentaho.ui.xul.containers.XulMenu;
import org.pentaho.ui.xul.dom.Document;


@SpoonPlugin(id = "AbsSpoonPlugin", image = "")
@SpoonPluginCategories({"spoon"})
public class AbsSpoonPlugin implements SpoonPluginInterface, SpoonLifecycleListener{
  
  private XulDomContainer spoonXulContainer = null;
  private RepositoryExplorerController repositoryExplorerEventHandler = new RepositoryExplorerController();
  private ChangedWarningController transChangedWarningEventHandler = new ChangedWarningController() {
    @Override
    public String getXulDialogId() {
      return "trans-graph-changed-warning-dialog"; //$NON-NLS-1$
    }
  };
  
  private ChangedWarningController jobChangedWarningEventHandler = new ChangedWarningController() {
    @Override
    public String getXulDialogId() {
      return "changed-warning-dialog"; //$NON-NLS-1$
    }
  };
  
  private ResourceBundle messages = new ResourceBundle() {

    @Override
    public Enumeration<String> getKeys() {
      return null;
    }

    @Override
    protected Object handleGetObject(String key) {
      return BaseMessages.getString(AbsSpoonPlugin.class, key);
    }
    
  }; 
  
  public AbsSpoonPlugin() {
    PluginLicenseVerifier.verify();
  }

  public SpoonLifecycleListener getLifecycleListener() {
    return this;
  }

  public SpoonPerspective getPerspective() {
    return null;
  }
  public void onEvent(SpoonLifeCycleEvent evt) {
    try {
      switch(evt) {
        case MENUS_REFRESHED:
          break;
        case REPOSITORY_CHANGED:
          doOnSecurityUpdate();
          break;
        case REPOSITORY_CONNECTED:
          doOnSecurityUpdate();
          break;
        case REPOSITORY_DISCONNECTED:
          doOnSecurityCleanup();
          break;
        case STARTUP:
          doOnStartup();
          break;
        case SHUTDOWN:
          doOnShutdown();
          break;
      }
    } catch (KettleException e) {
      try {
        if(Spoon.getInstance() != null) { // Make sure spoon has been initialized first
          if(spoonXulContainer == null) {
            spoonXulContainer = Spoon.getInstance().getMainSpoonContainer();
          }
          XulMessageBox messageBox = (XulMessageBox) spoonXulContainer.getDocumentRoot().createElement("messagebox");//$NON-NLS-1$
          messageBox.setTitle(messages.getString("Dialog.Success"));//$NON-NLS-1$
          messageBox.setAcceptLabel(messages.getString("Dialog.Ok"));//$NON-NLS-1$
          messageBox.setMessage(messages.getString("AbsController.RoleActionPermission.Success"));//$NON-NLS-1$
          messageBox.open();
        }
      } catch (Exception ex) {
        e.printStackTrace();
      }
    }
  }
  
  private void doOnStartup() {
    UIObjectRegistery.getInstance().registerUIRepositoryUserClass(UIEERepositoryUser.class);
    UIEEObjectRegistery.getInstance().registerUIRepositoryRoleClass(UIAbsRepositoryRole.class);
    registerRepositoryCapabilities();
  }
  
  private void doOnShutdown() {
  }
  
  /**
   * Override UI elements to reflect the users capabilities as described by their
   * permission levels
   */
  private void doOnSecurityUpdate() throws KettleException {
    if(Spoon.getInstance() != null) { // Make sure spoon has been initialized first
      if(spoonXulContainer == null) {
        spoonXulContainer = Spoon.getInstance().getMainSpoonContainer();
      }
      
      Object o = Spoon.getInstance().getSecurityManager();
      
      if(o instanceof IAbsSecurityProvider) {
        IAbsSecurityProvider securityProvider = (IAbsSecurityProvider)o;

        // Execute credential lookup
        enableCreatePermission(securityProvider.isAllowed(IAbsSecurityProvider.CREATE_CONTENT_ACTION));
        enableReadPermission(securityProvider.isAllowed(IAbsSecurityProvider.READ_CONTENT_ACTION));
        enableAdminPermission(securityProvider.isAllowed(IAbsSecurityProvider.ADMINISTER_SECURITY_ACTION));
      }
    }
  }
  private void doOnSecurityCleanup() {
  }
  
  private void enableCreatePermission(boolean createPermitted) {
    Document doc = spoonXulContainer.getDocumentRoot();
    
    // Main spoon toolbar
    ((XulToolbarbutton)doc.getElementById("toolbar-file-new")).setDisabled(!createPermitted); //$NON-NLS-1$
    ((XulToolbarbutton)doc.getElementById("toolbar-file-save")).setDisabled(!createPermitted); //$NON-NLS-1$
    ((XulToolbarbutton)doc.getElementById("toolbar-file-save-as")).setDisabled(!createPermitted); //$NON-NLS-1$
    
    // Popup menus
    ((XulMenuitem) doc.getElementById("trans-class-new")).setDisabled(!createPermitted); //$NON-NLS-1$
    ((XulMenuitem) doc.getElementById("job-class-new")).setDisabled(!createPermitted); //$NON-NLS-1$
    
    // Main spoon menu
    ((XulMenu) doc.getElementById("file-new")).setDisabled(!createPermitted); //$NON-NLS-1$
    ((XulMenuitem) doc.getElementById("file-save")).setDisabled(!createPermitted); //$NON-NLS-1$
    ((XulMenuitem) doc.getElementById("file-save-as")).setDisabled(!createPermitted); //$NON-NLS-1$
    ((XulMenuitem) doc.getElementById("file-close")).setDisabled(!createPermitted); //$NON-NLS-1$
    
    // Update repository explorer
    repositoryExplorerEventHandler.setCreatePermissionGranted(createPermitted);
    transChangedWarningEventHandler.setSavePermitted(createPermitted);
    jobChangedWarningEventHandler.setSavePermitted(createPermitted);
  }
  
  private void enableReadPermission(boolean readPermitted) {
    repositoryExplorerEventHandler.setReadPermissionGranted(readPermitted);
  }
  
  private void enableAdminPermission(boolean adminPermitted) {
  }
  
  private void registerRepositoryCapabilities() {
    UISupportRegistery.getInstance().registerUISupport(IRoleSupportSecurityManager.class, ManageRolesUISupport.class);
    UISupportRegistery.getInstance().registerUISupport(IAbsSecurityManager.class, AbsUISupport.class);
  }
  public void applyToContainer(String category, XulDomContainer container) throws XulException {
    // TODO Throwing a null pointer on setting repository in Connection Controller. Needs to take a look at how this will
    // work with the new plugin structure
    // container.addEventHandler(repositoryExplorerEventHandler);
    // container.addEventHandler(transChangedWarningEventHandler);
    // container.addEventHandler(jobChangedWarningEventHandler);
  }
}