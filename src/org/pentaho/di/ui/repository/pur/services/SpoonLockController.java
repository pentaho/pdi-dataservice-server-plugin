package org.pentaho.di.ui.repository.pur.services;

import java.util.Enumeration;
import java.util.List;
import java.util.ResourceBundle;

import org.eclipse.swt.graphics.Image;
import org.pentaho.di.core.EngineMetaInterface;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.IAbsSecurityProvider;
import org.pentaho.di.repository.RepositoryLock;
import org.pentaho.di.repository.pur.RepositoryLockService;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.repository.repositoryexplorer.RepositoryExplorer;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.job.JobGraph;
import org.pentaho.di.ui.spoon.trans.TransGraph;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.binding.BindingConvertor;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.binding.DefaultBindingFactory;
import org.pentaho.ui.xul.binding.Binding.Type;
import org.pentaho.ui.xul.components.XulMenuitem;
import org.pentaho.ui.xul.components.XulMessageBox;
import org.pentaho.ui.xul.components.XulPromptBox;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;
import org.pentaho.ui.xul.util.XulDialogCallback;

public class SpoonLockController extends AbstractXulEventHandler {
  
  private EngineMetaInterface workingMeta = null;
  
  private BindingFactory bindingFactory = null;
  
  private boolean tabBound = false;
  
  private boolean isCreateAllowed = false;
  
  private boolean isLockingAllowed = false;
  
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
    if(workingMeta != null) {
      // Bind the tab icon if it is not already bound (cannot be done in init because TransGraph must exist to create the tab)
      // Look in the SpoonTransformationDelegate for details on the TabItem creation
      if(!tabBound) {
        bindingFactory.createBinding(this, "activeMetaUnlocked", Spoon.getInstance().delegates.tabs.findTabMapEntry(workingMeta).getTabItem(), "image", new BindingConvertor<String, Image>() { //$NON-NLS-1$ //$NON-NLS-2$
          @Override
          public Image sourceToTarget(String activeMetaUnlocked) {
              if(Boolean.valueOf(activeMetaUnlocked)) {
                if(workingMeta instanceof TransMeta) {
                  return GUIResource.getInstance().getImageTransGraph();
                } else if(workingMeta instanceof JobMeta) {
                  return GUIResource.getInstance().getImageJobGraph();
                }
              } else {
                return GUIResource.getInstance().getImageLocked();
              }
              return null;
          }
    
          @Override
          public String targetToSource(Image arg0) {
            return null;
          }
        });
        tabBound = true;
      }
      
      // Decide whether to lock or unlock the object
      if(fetchRepositoryLock(workingMeta) == null) {
        // Lock the object (it currently is NOT locked)
        
        XulPromptBox lockNotePrompt = RepositoryExplorer.promptLockMessage(document, messages, null);
        lockNotePrompt.addDialogCallback(new XulDialogCallback<String>() {
          public void onClose(XulComponent component, Status status, String value) {
    
            if(!status.equals(Status.CANCEL)) {
              try {
                if(workingMeta instanceof TransMeta) {
                  Spoon.getInstance().getRepository().lockTransformation(workingMeta.getObjectId(), value);
                } else if(workingMeta instanceof JobMeta) {
                  Spoon.getInstance().getRepository().lockJob(workingMeta.getObjectId(), value);
                }
                
                // Execute binding. Notify listeners that the object is now locked
                firePropertyChange("activeMetaUnlocked", null, "false"); //$NON-NLS-1$ //$NON-NLS-2$
              } catch (Exception e) {
                // convert to runtime exception so it bubbles up through the UI
                throw new RuntimeException(e);
              }
            }
          }
    
          public void onError(XulComponent component, Throwable err) {
            throw new RuntimeException(err);
          }
        });
        
        lockNotePrompt.open();
      } else {
        // Unlock the object (it currently IS locked)
        if(workingMeta instanceof TransMeta) {
          Spoon.getInstance().getRepository().unlockTransformation(workingMeta.getObjectId());
        } else if(workingMeta instanceof JobMeta) {
          Spoon.getInstance().getRepository().unlockJob(workingMeta.getObjectId());
        }
        // Execute binding. Notify listeners that the object is now unlocked
        firePropertyChange("activeMetaUnlocked", null, "true"); //$NON-NLS-1$ //$NON-NLS-2$
      }
      
    }
  }
  
  public void viewLockNote() throws Exception {
    RepositoryLock repoLock = fetchRepositoryLock(workingMeta);
    if(repoLock != null) {
      XulMessageBox msgBox = (XulMessageBox) document.createElement("messagebox");  //$NON-NLS-1$
      msgBox.setTitle(messages.getString("PurRepository.LockNote.Title")); //$NON-NLS-1$
      msgBox.setMessage(repoLock.getMessage());
      
      msgBox.open();
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
      if((Spoon.getInstance().getRepository() != null) && (Spoon.getInstance().getRepository().hasService(IAbsSecurityProvider.class))) {
        IAbsSecurityProvider securityService = (IAbsSecurityProvider) Spoon.getInstance().getRepository().getService(IAbsSecurityProvider.class);
        
        setCreateAllowed(allowedActionsContains(securityService, IAbsSecurityProvider.CREATE_CONTENT_ACTION));
      }
      
      XulDomContainer container = getXulDomContainer();
      
      bindingFactory = new DefaultBindingFactory();
      bindingFactory.setDocument(container.getDocumentRoot());
      
      bindingFactory.setBindingType(Type.ONE_WAY);
      
      bindingFactory.createBinding(this, "activeMetaUnlocked", "lock-context-locknotes", "disabled"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
      bindingFactory.createBinding(this, "lockingNotAllowedAsString", "lock-context-lock", "disabled"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
      
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
        // Permit locking/unlocking if the user owns the lock
        setLockingAllowed(repoLock.getLogin().equalsIgnoreCase(Spoon.getInstance().getRepository().getUserInfo().getLogin()));
      } else {
        setLockingAllowed(true);
      }
      
      firePropertyChange("activeMetaUnlocked", null, repoLock != null ? "false" : "true"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  public boolean isCreateAllowed() {
    return isCreateAllowed;
  }

  public void setCreateAllowed(boolean isCreateAllowed) {
    this.isCreateAllowed = isCreateAllowed;
    this.firePropertyChange("createAllowed", null, isCreateAllowed); //$NON-NLS-1$
  }
  
  public boolean isLockingAllowed() {
    return isLockingAllowed;
  }
  
  public String isLockingNotAllowedAsString() {
    return Boolean.toString(!isLockingAllowed);
  }
  
  public void setLockingAllowed(boolean isLockingAllowed) {
    this.isLockingAllowed = isLockingAllowed;
    this.firePropertyChange("lockingNotAllowedAsString", null, Boolean.toString(!isLockingAllowed)); //$NON-NLS-1$
  }
  
  private boolean allowedActionsContains(IAbsSecurityProvider service, String action) throws KettleException {
    List<String> allowedActions = service.getAllowedActions(IAbsSecurityProvider.NAMESPACE);
    for (String actionName : allowedActions) {
      if (action != null && action.equals(actionName)) {
        return true;
      }
    }
    return false;
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
}