package org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.controller;

import org.pentaho.di.core.EngineMetaInterface;
import org.pentaho.di.core.logging.DefaultLogLevel;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.pur.PurRepository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.repository.EESpoonPlugin;
import org.pentaho.di.ui.repository.pur.services.IAbsSecurityProvider;
import org.pentaho.di.ui.spoon.ISpoonMenuController;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.ui.xul.components.XulMenuitem;
import org.pentaho.ui.xul.components.XulToolbarbutton;
import org.pentaho.ui.xul.containers.XulMenu;
import org.pentaho.ui.xul.dom.Document;

public class SpoonMenuABSController implements ISpoonMenuController {

  protected LogChannelInterface log;
  protected LogLevel logLevel = DefaultLogLevel.getLogLevel();

  public SpoonMenuABSController() {
    this.log = new LogChannel(this);
  }
  
  public String getName() {
    return "SpoonMenuABSController"; //$NON-NLS-1$
  }

  public void updateMenu(Document doc) {
    try {
      Spoon spoon = Spoon.getInstance();
      boolean createPermitted = true;
      
      // If we are working with an Enterprise Repository
      if((spoon != null) && (spoon.getRepository() != null) && (spoon.getRepository() instanceof PurRepository)) {
        Repository repo = spoon.getRepository();
        
        // Check for ABS Security
        if (repo.hasService(IAbsSecurityProvider.class)) {
          IAbsSecurityProvider securityProvider = (IAbsSecurityProvider) repo.getService(IAbsSecurityProvider.class);
          
          // Get write permission
          createPermitted = securityProvider.isAllowed(IAbsSecurityProvider.CREATE_CONTENT_ACTION);
          
          EngineMetaInterface meta = spoon.getActiveMeta();
          
          // If (meta is not null) and (meta is either a Transformation or Job)
          if((meta != null) && ((meta instanceof JobMeta) || (meta instanceof TransMeta))) {
          
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
      }

      EESpoonPlugin.updateChangedWarningDialog(createPermitted);
      
    } catch(Exception e) {
      // don't let this bomb all the way out, otherwise we'll get stuck:  PDI-4670
      log.logError(e.getMessage(), e);
    }
  }

}
