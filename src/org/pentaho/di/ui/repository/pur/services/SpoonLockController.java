package org.pentaho.di.ui.repository.pur.services;

import java.util.Enumeration;
import java.util.ResourceBundle;

import org.pentaho.di.core.EngineMetaInterface;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.RepositoryLock;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.job.JobGraph;
import org.pentaho.di.ui.spoon.trans.TransGraph;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.binding.DefaultBindingFactory;
import org.pentaho.ui.xul.binding.Binding.Type;
import org.pentaho.ui.xul.components.XulMenuitem;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

public class SpoonLockController extends AbstractXulEventHandler {
  
  private EngineMetaInterface workingMeta = null;
  
  protected ResourceBundle messages = new ResourceBundle() {

    @Override
    public Enumeration<String> getKeys() {
      return null;
    }

    @Override
    protected Object handleGetObject(String key) {
      return BaseMessages.getString(RepositoryLockService.class, key);
    }
  };
  
  public String getName() {
    return "spoonLockController"; //$NON-NLS-1$
  }
  
  public void lockContent() throws Exception {
    boolean result = lockMeta(workingMeta);

    firePropertyChange("activeMetaUnlocked", null, result == true ? "false" : "true"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
  }
  
  public void viewLockNote() throws Exception {
    RepositoryLock repoLock = fetchRepositoryLock(workingMeta);
    if(repoLock != null) {
      String msg = repoLock.getMessage();
      //TODO: throw xul popup message
      System.out.println(msg);
    }
  }
  
  @Override
  public void setXulDomContainer(XulDomContainer xulDomContainer) {
    super.setXulDomContainer(xulDomContainer);
    init();
  }
  
  public String isActiveMetaUnlocked() {
    try {
      if(fetchRepositoryLock(workingMeta) != null) {
        return Boolean.toString(false);
      } else {
        return Boolean.toString(true);
      }
    } catch (KettleException e) {
      throw new RuntimeException(e);
    }
  }
  
  protected void init() {
    try {
      XulDomContainer container = getXulDomContainer();
      
      BindingFactory bindingFactory = new DefaultBindingFactory();
      bindingFactory.setDocument(container.getDocumentRoot());
      
      bindingFactory.setBindingType(Type.ONE_WAY);
      
      bindingFactory.createBinding(this, "activeMetaUnlocked", "lock-context-locknotes", "disabled"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
      
      // Get trans* object to gain access to the *Meta object to determine if we are initially locked or not
      // Try transformation
      if(container.getEventHandlers().containsKey("transgraph")) { //$NON-NLS-1$
        workingMeta = ((TransGraph)container.getEventHandler("transgraph")).getMeta(); //$NON-NLS-1$
      } else if(container.getEventHandlers().containsKey("jobgraph")) { //$NON-NLS-1$
        workingMeta = ((JobGraph)container.getEventHandler("jobgraph")).getMeta(); //$NON-NLS-1$
      }

      RepositoryLock repoLock = fetchRepositoryLock(workingMeta);
      if(repoLock != null) {
        XulMenuitem lockMenuItem = (XulMenuitem)container.getDocumentRoot().getElementById("lock-context-lock"); //$NON-NLS-1$
        lockMenuItem.setSelected(true);
      }
      
      firePropertyChange("activeMetaUnlocked", null, repoLock != null ? "false" : "true"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  protected RepositoryLock fetchRepositoryLock(EngineMetaInterface meta) throws KettleException {
    RepositoryLock result = null;
    if(meta != null) {
      if(meta.getObjectId() != null) {
        if(meta instanceof TransMeta) {
          result = Spoon.getInstance().getRepository().getTransformationLock(meta.getObjectId());
        } else if(meta instanceof JobMeta) {
          result = Spoon.getInstance().getRepository().getJobLock(meta.getObjectId());
        }
      }
    }
    return result;
  }
  
  protected boolean lockMeta(EngineMetaInterface meta) throws KettleException {
    if(meta != null) {
      RepositoryLock repoLock = fetchRepositoryLock(meta);
      if(meta instanceof TransMeta) {
        if(repoLock != null) {
          Spoon.getInstance().getRepository().unlockTransformation(meta.getObjectId());
          return false;
        } else {
          Spoon.getInstance().getRepository().lockTransformation(meta.getObjectId(), "Lock Note");
          return true;
        }
      } else if(meta instanceof JobMeta) {
        if(repoLock != null) {
          Spoon.getInstance().getRepository().unlockJob(meta.getObjectId());
          return false;
        } else {
          Spoon.getInstance().getRepository().lockJob(meta.getObjectId(), "Lock Note");
          return true;
        }
      }
    }
    return false;
  }
}