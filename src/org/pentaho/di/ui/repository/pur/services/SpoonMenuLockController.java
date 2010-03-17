package org.pentaho.di.ui.repository.pur.services;

import org.pentaho.di.core.EngineMetaInterface;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryLock;
import org.pentaho.di.repository.pur.PurRepository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.spoon.ISpoonMenuController;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.ui.xul.components.XulMenuitem;
import org.pentaho.ui.xul.components.XulToolbarbutton;
import org.pentaho.ui.xul.dom.Document;

public class SpoonMenuLockController implements ISpoonMenuController {


  public String getName() {
    return "spoonMenuLockController"; //$NON-NLS-1$
  }

  public void updateMenu(Document doc) {
    try {
      Spoon spoon = Spoon.getInstance();
      
      // If we are working with an Enterprise Repository
      if((spoon != null) && (spoon.getRepository() != null) && (spoon.getRepository() instanceof PurRepository)) {
        Repository repo = spoon.getRepository();
        
        EngineMetaInterface meta = spoon.getActiveMeta();
        
        // If (meta is not null) and (meta is either a Transformation or Job)
        if((meta != null) && ((meta instanceof JobMeta) || (meta instanceof TransMeta))) {
        
          RepositoryLock repoLock = null;
          if(repo != null  && meta.getObjectId() != null) {
            if(meta instanceof JobMeta) {
              repoLock = repo.getJobLock(meta.getObjectId());
            } else {
              repoLock = repo.getTransformationLock(meta.getObjectId());
            }
          }
          // If (there is a lock on this item) and (the UserInfo is unavailable or the current user does not match the lock owner)
          if((repoLock != null) && (repo.getUserInfo() == null || !repoLock.getLogin().equals(repo.getUserInfo().getLogin()))) {
            // User does not have modify permissions on this file
            ((XulToolbarbutton)doc.getElementById("toolbar-file-save")).setDisabled(true); //$NON-NLS-1$
            ((XulMenuitem)doc.getElementById("file-save")).setDisabled(true); //$NON-NLS-1$
          }
        }
      }
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
  }
}
