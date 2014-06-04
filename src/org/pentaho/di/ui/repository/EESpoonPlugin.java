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

package org.pentaho.di.ui.repository;

import org.eclipse.swt.SWT;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.pur.PluginLicenseVerifier;
import org.pentaho.di.repository.pur.PurRepository;
import org.pentaho.di.ui.repository.pur.controller.SpoonLockController;
import org.pentaho.di.ui.repository.pur.controller.SpoonMenuLockController;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.UIEEObjectRegistery;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.AbsSecurityManagerUISupport;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.AbsSecurityProviderUISupport;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.controller.SpoonMenuABSController;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.model.UIAbsRepositoryRole;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.model.UIEEDatabaseConnection;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.model.UIEEJob;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.model.UIEERepositoryDirectory;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.model.UIEERepositoryUser;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.model.UIEETransformation;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.uisupport.AclUISupport;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.uisupport.ConnectionAclUISupport;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.uisupport.ManageRolesUISupport;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.uisupport.RepositoryLockUISupport;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.uisupport.RevisionsUISupport;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.uisupport.TrashUISupport;
import org.pentaho.di.ui.repository.pur.services.IAbsSecurityManager;
import org.pentaho.di.ui.repository.pur.services.IAbsSecurityProvider;
import org.pentaho.di.ui.repository.pur.services.IAclService;
import org.pentaho.di.ui.repository.pur.services.IConnectionAclService;
import org.pentaho.di.ui.repository.pur.services.ILockService;
import org.pentaho.di.ui.repository.pur.services.IRevisionService;
import org.pentaho.di.ui.repository.pur.services.IRoleSupportSecurityManager;
import org.pentaho.di.ui.repository.pur.services.ITrashService;
import org.pentaho.di.ui.repository.repositoryexplorer.UISupportRegistery;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIObjectRegistry;
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

import java.util.Enumeration;
import java.util.ResourceBundle;
// LICENSE CHECK

@SpoonPlugin( id = "EESpoonPlugin", image = "" )
@SpoonPluginCategories( { "spoon", "trans-graph", "job-graph" } )
public class EESpoonPlugin implements SpoonPluginInterface, SpoonLifecycleListener, java.io.Serializable {

  private static final long serialVersionUID = -5672306503357631444L; /* EESOURCE: UPDATE SERIALVERUID */

  protected static Class<?> PKG = EESpoonPlugin.class;

  protected XulDomContainer spoonXulContainer = null;

  protected ResourceBundle messages = new ResourceBundle() {

    @Override
    public Enumeration<String> getKeys() {
      return null;
    }

    @Override
    protected Object handleGetObject( String key ) {
      return BaseMessages.getString( PKG, key );
    }

  };

  public EESpoonPlugin() {
    PluginLicenseVerifier.verify(); // LICENSE CHECK
  }

  public SpoonLifecycleListener getLifecycleListener() {
    return this;
  }

  public SpoonPerspective getPerspective() {
    return null;
  }

  public void onEvent( SpoonLifeCycleEvent evt ) {
    try {
      switch ( evt ) {
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
        default:
          break;
      }
    } catch ( KettleException e ) {
      try {
        getMainSpoonContainer();
        XulMessageBox messageBox = (XulMessageBox) spoonXulContainer.getDocumentRoot()
                                                                    .createElement( "messagebox" );//$NON-NLS-1$
        messageBox.setTitle( BaseMessages.getString( PKG, "Dialog.Success" ) );//$NON-NLS-1$
        messageBox.setAcceptLabel( BaseMessages.getString( PKG, "Dialog.Ok" ) );//$NON-NLS-1$
        messageBox
            .setMessage( BaseMessages.getString( PKG, "AbsController.RoleActionPermission.Success" ) );//$NON-NLS-1$
        messageBox.open();
      } catch ( Exception ex ) {
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
    if ( repository != null && repository.hasService( IRoleSupportSecurityManager.class ) ) {
      UIObjectRegistry.getInstance().registerUIRepositoryUserClass( UIEERepositoryUser.class );
    } else {
      UIObjectRegistry.getInstance().registerUIRepositoryUserClass( UIObjectRegistry.DEFAULT_UIREPOSITORYUSER_CLASS );
    }
    // Repository Directory
    if ( repository != null && repository.hasService( IAclService.class ) ) {
      UIObjectRegistry.getInstance().registerUIRepositoryDirectoryClass( UIEERepositoryDirectory.class );
      UIObjectRegistry.getInstance().registerUIDatabaseConnectionClass( UIEEDatabaseConnection.class );

    } else {
      UIObjectRegistry.getInstance().registerUIRepositoryDirectoryClass( UIObjectRegistry.DEFAULT_UIDIR_CLASS );
    }
    // Repository Role
    if ( repository != null && repository.hasService( IAbsSecurityProvider.class ) ) {
      UIEEObjectRegistery.getInstance().registerUIRepositoryRoleClass( UIAbsRepositoryRole.class );
      IAbsSecurityProvider securityProvider = (IAbsSecurityProvider) repository
          .getService( IAbsSecurityProvider.class );

      enableCreatePermission( securityProvider.isAllowed( IAbsSecurityProvider.CREATE_CONTENT_ACTION ) );
      enableAdminPermission( securityProvider.isAllowed( IAbsSecurityProvider.ADMINISTER_SECURITY_ACTION ) );
      enableExecutePermission( securityProvider.isAllowed( IAbsSecurityProvider.EXECUTE_CONTENT_ACTION ) );
    }
    // Job & Transformation =
    if ( repository.hasService( ILockService.class ) ) {
      UIObjectRegistry.getInstance().registerUIJobClass( UIEEJob.class );
      UIObjectRegistry.getInstance().registerUITransformationClass( UIEETransformation.class );
      SpoonDelegateRegistry.getInstance().registerSpoonJobDelegateClass( SpoonEEJobDelegate.class );
      SpoonDelegateRegistry.getInstance().registerSpoonTransDelegateClass( SpoonEETransformationDelegate.class );
    } else {
      UIObjectRegistry.getInstance().registerUIJobClass( UIObjectRegistry.DEFAULT_UIJOB_CLASS );
      UIObjectRegistry.getInstance().registerUITransformationClass( UIObjectRegistry.DEFAULT_UITRANS_CLASS );
      SpoonDelegateRegistry.getInstance()
                           .registerSpoonJobDelegateClass( SpoonDelegateRegistry.DEFAULT_SPOONJOBDELEGATE_CLASS );
      SpoonDelegateRegistry.getInstance()
                           .registerSpoonTransDelegateClass( SpoonDelegateRegistry.DEFAULT_SPOONTRANSDELEGATE_CLASS );
    }
  }

  /**
   * Called when repository is disconnected.
   */
  private void doOnSecurityCleanup() {
    updateMenuState( true );
    updateExecuteMenuState( false );
  }

  private void enableCreatePermission( boolean createPermitted ) {
    updateMenuState( createPermitted );
    updateChangedWarningDialog( createPermitted );
  }

  private void enableExecutePermission( boolean executePermitted ) {
    updateExecuteMenuState( executePermitted );
  }

  private void enableAdminPermission( boolean adminPermitted ) {
  }

  private void registerUISuppportForRepositoryExplorer() {
    UISupportRegistery.getInstance().registerUISupport( IRevisionService.class, RevisionsUISupport.class );
    UISupportRegistery.getInstance().registerUISupport( IAclService.class, AclUISupport.class );
    UISupportRegistery.getInstance().registerUISupport( IConnectionAclService.class, ConnectionAclUISupport.class );
    UISupportRegistery.getInstance().registerUISupport( IRoleSupportSecurityManager.class, ManageRolesUISupport.class );
    UISupportRegistery.getInstance().registerUISupport( IAbsSecurityManager.class, AbsSecurityManagerUISupport.class );
    UISupportRegistery.getInstance()
                      .registerUISupport( IAbsSecurityProvider.class, AbsSecurityProviderUISupport.class );
    UISupportRegistery.getInstance().registerUISupport( ITrashService.class, TrashUISupport.class );
    UISupportRegistery.getInstance().registerUISupport( ILockService.class, RepositoryLockUISupport.class );
  }

  public void applyToContainer( String category, XulDomContainer container ) throws XulException {
    container.registerClassLoader( getClass().getClassLoader() );
    if ( category.equals( "spoon" ) ) { //$NON-NLS-1$

      // register the two controllers, note that the lock controller must come 
      // after the abs controller so the biz logic between the two hold.

      // Register the ABS Menu controller
      Spoon.getInstance().addSpoonMenuController( new SpoonMenuABSController() );

      // Register the SpoonMenuLockController to modify the main Spoon Menu structure
      Spoon.getInstance().addSpoonMenuController( new SpoonMenuLockController() );

    } else if ( category.equals( "trans-graph" ) || category.equals( "job-graph" ) ) { //$NON-NLS-1$ //$NON-NLS-2$
      if ( ( Spoon.getInstance() != null ) && ( Spoon.getInstance().getRepository() != null ) && ( Spoon.getInstance()
                                                                                                        .getRepository() instanceof PurRepository ) ) {
        container.getDocumentRoot()
                 .addOverlay( "org/pentaho/di/ui/repository/pur/xul/spoon-lock-overlay.xul" ); //$NON-NLS-1$
        container.addEventHandler( new SpoonLockController() );
      }

      try {
        Repository repository = Spoon.getInstance().getRepository();
        if ( repository != null ) {
          IAbsSecurityProvider securityProvider = (IAbsSecurityProvider) repository
              .getService( IAbsSecurityProvider.class );
          if ( securityProvider != null ) {
            enableExecutePermission( securityProvider.isAllowed( IAbsSecurityProvider.EXECUTE_CONTENT_ACTION ) );
          }
        }
      } catch ( KettleException e ) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Change the menu-item states based on Execute permissions.
   *
   * @param executePermitted
   *     - if true, we enable menu-items
   */
  private void updateExecuteMenuState( boolean executePermitted ) {
    Document doc = getDocumentRoot();
    if ( doc != null ) {
      // Main spoon menu
      ( (XulMenuitem) doc.getElementById( "process-run" ) ).setDisabled( !executePermitted ); //$NON-NLS-1$
      ( (XulMenuitem) doc.getElementById( "trans-preview" ) ).setDisabled( !executePermitted ); //$NON-NLS-1$
      ( (XulMenuitem) doc.getElementById( "trans-replay" ) ).setDisabled( !executePermitted ); //$NON-NLS-1$
      ( (XulMenuitem) doc.getElementById( "trans-verify" ) ).setDisabled( !executePermitted ); //$NON-NLS-1$
      ( (XulMenuitem) doc.getElementById( "trans-impact" ) ).setDisabled( !executePermitted ); //$NON-NLS-1$
      ( (XulMenuitem) doc.getElementById( "trans-get-sql" ) ).setDisabled( !executePermitted ); //$NON-NLS-1$

      // Schedule is a plugin
      if ( doc.getElementById( "trans-schedule" ) != null ) {
        ( (XulMenuitem) doc.getElementById( "trans-schedule" ) ).setDisabled( !executePermitted ); //$NON-NLS-1$
      }
    }
  }

  private void updateMenuState( boolean createPermitted ) {
    Document doc = getDocumentRoot();
    if ( doc != null ) {
      // Main spoon toolbar
      ( (XulToolbarbutton) doc.getElementById( "toolbar-file-new" ) ).setDisabled( !createPermitted ); //$NON-NLS-1$
      ( (XulToolbarbutton) doc.getElementById( "toolbar-file-save" ) ).setDisabled( !createPermitted ); //$NON-NLS-1$
      ( (XulToolbarbutton) doc.getElementById( "toolbar-file-save-as" ) ).setDisabled( !createPermitted ); //$NON-NLS-1$

      // Popup menus
      ( (XulMenuitem) doc.getElementById( "trans-class-new" ) ).setDisabled( !createPermitted ); //$NON-NLS-1$
      ( (XulMenuitem) doc.getElementById( "job-class-new" ) ).setDisabled( !createPermitted ); //$NON-NLS-1$

      // Main spoon menu
      ( (XulMenu) doc.getElementById( "file-new" ) ).setDisabled( !createPermitted ); //$NON-NLS-1$
      ( (XulMenuitem) doc.getElementById( "file-save" ) ).setDisabled( !createPermitted ); //$NON-NLS-1$
      ( (XulMenuitem) doc.getElementById( "file-save-as" ) ).setDisabled( !createPermitted ); //$NON-NLS-1$
      ( (XulMenuitem) doc.getElementById( "file-close" ) ).setDisabled( !createPermitted ); //$NON-NLS-1$
    }
  }

  public static void  updateChangedWarningDialog( boolean overrideDefaultDialogBehavior ) {
    if ( !overrideDefaultDialogBehavior ) {
      // Update the ChangedWarningDialog - Disable the yes button
      ChangedWarningDialog.setInstance( new ChangedWarningDialog() {

        public int show() {
          return show( null );
        }

        public int show( String fileName ) {
          XulMessageBox msgBox = null;
          try {
            msgBox = runXulChangedWarningDialog( fileName );
            if ( fileName != null ) {
              msgBox.setMessage( BaseMessages
                  .getString( PKG, "Spoon.Dialog.PromptToSave.Fail.Message.WithParam", fileName ) ); //$NON-NLS-1$
            } else {
              msgBox
                  .setMessage( BaseMessages.getString( PKG, "Spoon.Dialog.PromptToSave.Fail.Message" ) ); //$NON-NLS-1$
            }

            msgBox.setButtons( new Integer[] { SWT.YES | SWT.NO } );
          } catch ( Exception e ) {
            e.printStackTrace();
            throw new RuntimeException( e );
          }
          int retVal = msgBox.open();

          // Map from this question to make sense in the original context (Yes = save, No = no-save , Cancel = do not disconnect)
          if ( retVal == SWT.YES ) {
            return SWT.NO;
          } else {
            return SWT.CANCEL;
          }
        }
      } );
    } else {
      ChangedWarningDialog.setInstance( new ChangedWarningDialog() );
    }
  }

  private Document getDocumentRoot() {
    getMainSpoonContainer();
    if ( spoonXulContainer != null ) {
      return spoonXulContainer.getDocumentRoot();
    } else {
      return null;
    }

  }

  private void getMainSpoonContainer() {
    if ( Spoon.getInstance() != null ) { // Make sure spoon has been initialized first
      if ( spoonXulContainer == null && Spoon.getInstance().getMainSpoonContainer() != null ) {
        spoonXulContainer = Spoon.getInstance().getMainSpoonContainer();
      }
    }
  }
}
