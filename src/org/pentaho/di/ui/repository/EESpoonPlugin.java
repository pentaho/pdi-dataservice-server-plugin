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
package org.pentaho.di.ui.repository;

import java.util.Enumeration;
import java.util.ResourceBundle;

import org.eclipse.swt.SWT;
import org.pentaho.di.core.DomainObjectRegistry;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.model.EEJobMeta;
import org.pentaho.di.repository.model.EETransMeta;
import org.pentaho.di.repository.pur.PluginLicenseVerifier;
import org.pentaho.di.repository.pur.PurRepository;
import org.pentaho.di.repository.services.IAbsSecurityManager;
import org.pentaho.di.repository.services.IAbsSecurityProvider;
import org.pentaho.di.repository.services.IAclService;
import org.pentaho.di.repository.services.ILockService;
import org.pentaho.di.repository.services.IRevisionService;
import org.pentaho.di.repository.services.IRoleSupportSecurityManager;
import org.pentaho.di.repository.services.ITrashService;
import org.pentaho.di.ui.repository.pur.services.SpoonLockController;
import org.pentaho.di.ui.repository.pur.services.SpoonMenuLockController;
import org.pentaho.di.ui.repository.repositoryexplorer.AclUISupport;
import org.pentaho.di.ui.repository.repositoryexplorer.ManageRolesUISupport;
import org.pentaho.di.ui.repository.repositoryexplorer.RepositoryLockUISupport;
import org.pentaho.di.ui.repository.repositoryexplorer.RevisionsUISupport;
import org.pentaho.di.ui.repository.repositoryexplorer.UIEEObjectRegistery;
import org.pentaho.di.ui.repository.repositoryexplorer.UISupportRegistery;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.AbsSecurityManagerUISupport;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.AbsSecurityProviderUISupport;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.controller.SpoonMenuABSController;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.model.UIAbsRepositoryRole;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIEEJob;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIEERepositoryDirectory;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIEERepositoryUser;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIEETransformation;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIObjectRegistry;
import org.pentaho.di.ui.repository.repositoryexplorer.trash.TrashUISupport;
import org.pentaho.di.ui.spoon.ChangedWarningDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.SpoonLifecycleListener;
import org.pentaho.di.ui.spoon.SpoonPerspective;
import org.pentaho.di.ui.spoon.SpoonPlugin;
import org.pentaho.di.ui.spoon.SpoonPluginCategories;
import org.pentaho.di.ui.spoon.SpoonPluginInterface;
import org.pentaho.di.ui.spoon.delegates.SpoonDelegateRegistry;
import org.pentaho.di.ui.spoon.delegates.SpoonEEJobDelegate;
import org.pentaho.di.ui.spoon.delegates.SpoonEETransformationDelegate;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.components.XulMenuitem;
import org.pentaho.ui.xul.components.XulMessageBox;
import org.pentaho.ui.xul.components.XulToolbarbutton;
import org.pentaho.ui.xul.containers.XulMenu;
import org.pentaho.ui.xul.dom.Document;

@SpoonPlugin(id = "EESpoonPlugin", image = "")
@SpoonPluginCategories( { "spoon", "trans-graph", "job-graph" })
public class EESpoonPlugin implements SpoonPluginInterface, SpoonLifecycleListener {

  private XulDomContainer spoonXulContainer = null;

  private ResourceBundle messages = new ResourceBundle() {

    @Override
    public Enumeration<String> getKeys() {
      return null;
    }

    @Override
    protected Object handleGetObject(String key) {
      return BaseMessages.getString(EESpoonPlugin.class, key);
    }

  };

  public EESpoonPlugin() {
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
      switch (evt) {
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
        getMainSpoonContainer();
        XulMessageBox messageBox = (XulMessageBox) spoonXulContainer.getDocumentRoot().createElement("messagebox");//$NON-NLS-1$
        messageBox.setTitle(messages.getString("Dialog.Success"));//$NON-NLS-1$
        messageBox.setAcceptLabel(messages.getString("Dialog.Ok"));//$NON-NLS-1$
        messageBox.setMessage(messages.getString("AbsController.RoleActionPermission.Success"));//$NON-NLS-1$
        messageBox.open();
      } catch (Exception ex) {
        e.printStackTrace();
      }
    }
  }

  private void doOnStartup() {
    registerUISuppportForRepositoryExplorer();
  }

  private void doOnShutdown() {
  }

  /**
   * Override UI elements to reflect the users capabilities as described by their
   * permission levels
   */
  private void doOnSecurityUpdate() throws KettleException {
    getMainSpoonContainer();
    Repository repository = Spoon.getInstance().getRepository();
    // Repository User
    if(repository != null && repository.hasService(IRoleSupportSecurityManager.class)) {
      UIObjectRegistry.getInstance().registerUIRepositoryUserClass(UIEERepositoryUser.class);      
    } else {
      UIObjectRegistry.getInstance().registerUIRepositoryUserClass(UIObjectRegistry.DEFAULT_UIREPOSITORYUSER_CLASS);
    }
    // Repository Directory
    if(repository != null && repository.hasService(IAclService.class)) {
      UIObjectRegistry.getInstance().registerUIRepositoryDirectoryClass(UIEERepositoryDirectory.class);
    } else {
      UIObjectRegistry.getInstance().registerUIRepositoryDirectoryClass(UIObjectRegistry.DEFAULT_UIDIR_CLASS);
    }
    // Repository Role
    if (repository != null && repository.hasService(IAbsSecurityProvider.class)) {
      UIEEObjectRegistery.getInstance().registerUIRepositoryRoleClass(UIAbsRepositoryRole.class);
      IAbsSecurityProvider securityProvider = (IAbsSecurityProvider) repository.getService(IAbsSecurityProvider.class);
      // Execute credential lookup
      enableCreatePermission(securityProvider.isAllowed(IAbsSecurityProvider.CREATE_CONTENT_ACTION));
      enableAdminPermission(securityProvider.isAllowed(IAbsSecurityProvider.ADMINISTER_SECURITY_ACTION));
    }
    // Job & Transformation =
    if(repository.hasService(ILockService.class)) {
      UIObjectRegistry.getInstance().registerUIJobClass(UIEEJob.class);
      UIObjectRegistry.getInstance().registerUITransformationClass(UIEETransformation.class);
      SpoonDelegateRegistry.getInstance().registerSpoonJobDelegateClass(SpoonEEJobDelegate.class);
      SpoonDelegateRegistry.getInstance().registerSpoonTransDelegateClass(SpoonEETransformationDelegate.class);
      DomainObjectRegistry.getInstance().registerJobMetaClass(EEJobMeta.class);
      DomainObjectRegistry.getInstance().registerTransMetaClass(EETransMeta.class);
    } else {
      UIObjectRegistry.getInstance().registerUIJobClass(UIObjectRegistry.DEFAULT_UIJOB_CLASS);
      UIObjectRegistry.getInstance().registerUITransformationClass(UIObjectRegistry.DEFAULT_UITRANS_CLASS);
      SpoonDelegateRegistry.getInstance().registerSpoonJobDelegateClass(SpoonDelegateRegistry.DEFAULT_SPOONJOBDELEGATE_CLASS);
      SpoonDelegateRegistry.getInstance().registerSpoonTransDelegateClass(SpoonDelegateRegistry.DEFAULT_SPOONTRANSDELEGATE_CLASS);
      DomainObjectRegistry.getInstance().registerJobMetaClass(DomainObjectRegistry.DEFAULT_JOB_META_CLASS);
      DomainObjectRegistry.getInstance().registerTransMetaClass(DomainObjectRegistry.DEFAULT_TRANS_META_CLASS);
    }
  }

  private void doOnSecurityCleanup() {
    updateMenuState(true);
    updateChangedWarningDialog(true);
  }

  private void enableCreatePermission(boolean createPermitted) {
    updateMenuState(createPermitted);
    updateChangedWarningDialog(createPermitted);
  }

  private void enableAdminPermission(boolean adminPermitted) {
  }

  private void registerUISuppportForRepositoryExplorer() {
    UISupportRegistery.getInstance().registerUISupport(IRevisionService.class, RevisionsUISupport.class);
    UISupportRegistery.getInstance().registerUISupport(IAclService.class, AclUISupport.class);
    UISupportRegistery.getInstance().registerUISupport(IRoleSupportSecurityManager.class, ManageRolesUISupport.class);
    UISupportRegistery.getInstance().registerUISupport(IAbsSecurityManager.class, AbsSecurityManagerUISupport.class);
    UISupportRegistery.getInstance().registerUISupport(IAbsSecurityProvider.class, AbsSecurityProviderUISupport.class);
    UISupportRegistery.getInstance().registerUISupport(ITrashService.class, TrashUISupport.class);
    UISupportRegistery.getInstance().registerUISupport(ILockService.class, RepositoryLockUISupport.class);
  }

  public void applyToContainer(String category, XulDomContainer container) throws XulException {
    container.registerClassLoader(getClass().getClassLoader());
    if (category.equals("spoon")) { //$NON-NLS-1$

      // register the two controllers, note that the lock controller must come 
      // after the abs controller so the biz logic between the two hold.
      
      // Register the ABS Menu controller
      Spoon.getInstance().addSpoonMenuController(new SpoonMenuABSController());

      // Register the SpoonMenuLockController to modify the main Spoon Menu structure
      Spoon.getInstance().addSpoonMenuController(new SpoonMenuLockController());

    } else if (category.equals("trans-graph") || category.equals("job-graph")) { //$NON-NLS-1$ //$NON-NLS-2$
      if ((Spoon.getInstance() != null) && (Spoon.getInstance().getRepository() != null)
          && (Spoon.getInstance().getRepository() instanceof PurRepository)) {
        container.getDocumentRoot().addOverlay("org/pentaho/di/ui/repository/pur/xul/spoon-lock-overlay.xul"); //$NON-NLS-1$
        container.addEventHandler(new SpoonLockController());
      }
    }
  }

  private void updateMenuState(boolean createPermitted) {
    Document doc = getDocumentRoot();
    if(doc != null) {
      // Main spoon toolbar
      ((XulToolbarbutton) doc.getElementById("toolbar-file-new")).setDisabled(!createPermitted); //$NON-NLS-1$
      ((XulToolbarbutton) doc.getElementById("toolbar-file-save")).setDisabled(!createPermitted); //$NON-NLS-1$
      ((XulToolbarbutton) doc.getElementById("toolbar-file-save-as")).setDisabled(!createPermitted); //$NON-NLS-1$
  
      // Popup menus
      ((XulMenuitem) doc.getElementById("trans-class-new")).setDisabled(!createPermitted); //$NON-NLS-1$
      ((XulMenuitem) doc.getElementById("job-class-new")).setDisabled(!createPermitted); //$NON-NLS-1$
  
      // Main spoon menu
      ((XulMenu) doc.getElementById("file-new")).setDisabled(!createPermitted); //$NON-NLS-1$
      ((XulMenuitem) doc.getElementById("file-save")).setDisabled(!createPermitted); //$NON-NLS-1$
      ((XulMenuitem) doc.getElementById("file-save-as")).setDisabled(!createPermitted); //$NON-NLS-1$
      ((XulMenuitem) doc.getElementById("file-close")).setDisabled(!createPermitted); //$NON-NLS-1$
    }
  }
  
  public static void updateChangedWarningDialog(boolean createPermitted) {
    if(!createPermitted) {
      // Update the ChangedWarningDialog - Disable the yes button
      ChangedWarningDialog.setInstance(new ChangedWarningDialog() {
        private Class<?> PKG = EESpoonPlugin.class;
        
        public int show() {
          return show(null);
        }
        
        public int show(String fileName) {
          XulMessageBox msgBox = null;
          try {
            msgBox = runXulChangedWarningDialog(fileName);
            if(fileName != null) {
              msgBox.setMessage(BaseMessages.getString(PKG, "Spoon.Dialog.PromptToSave.Fail.Message.WithParam", fileName)); //$NON-NLS-1$
            } else {
              msgBox.setMessage(BaseMessages.getString(PKG, "Spoon.Dialog.PromptToSave.Fail.Message")); //$NON-NLS-1$
            }

            msgBox.setButtons(new Integer[] {SWT.YES | SWT.NO});
          } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
          int retVal = msgBox.open();
         
          // Map from this question to make sense in the original context (Yes = save, No = no-save , Cancel = do not close)
          if(retVal == SWT.YES) {
            return SWT.NO;
          } else {
            return SWT.CANCEL;
          }
        }
      });
    } else {
      ChangedWarningDialog.setInstance(new ChangedWarningDialog());
    }
  }
  
  private Document getDocumentRoot() {
    getMainSpoonContainer();
    if(spoonXulContainer != null) {
      return spoonXulContainer.getDocumentRoot();  
    } else {
      return null;  
    }
     
  }
  private void getMainSpoonContainer() {
    if (Spoon.getInstance() != null) { // Make sure spoon has been initialized first
      if (spoonXulContainer == null && Spoon.getInstance().getMainSpoonContainer() != null) {
        spoonXulContainer = Spoon.getInstance().getMainSpoonContainer();
      }
    }
  }
}