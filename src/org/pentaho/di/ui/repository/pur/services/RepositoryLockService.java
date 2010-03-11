package org.pentaho.di.ui.repository.pur.services;

import java.util.Enumeration;
import java.util.List;
import java.util.ResourceBundle;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.IRepositoryService;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.repository.capabilities.AbstractRepositoryExplorerUISupport;
import org.pentaho.di.ui.repository.repositoryexplorer.ControllerInitializationException;
import org.pentaho.di.ui.repository.repositoryexplorer.IUISupportController;
import org.pentaho.di.ui.repository.repositoryexplorer.RepositoryExplorer;
import org.pentaho.di.ui.repository.repositoryexplorer.controllers.BrowseController;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryContent;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryObject;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.binding.BindingConvertor;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.binding.DefaultBindingFactory;
import org.pentaho.ui.xul.components.XulPromptBox;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;
import org.pentaho.ui.xul.impl.DefaultXulOverlay;
import org.pentaho.ui.xul.impl.XulEventHandler;
import org.pentaho.ui.xul.util.XulDialogCallback;

public class RepositoryLockService extends AbstractRepositoryExplorerUISupport implements IRepositoryService {

  public class RepositoryLockController extends AbstractXulEventHandler implements IUISupportController {
    
    private BrowseController browseController = null;
    
    private BindingFactory bindingFactory = null;
    
    protected ResourceBundle messages = new ResourceBundle() {

      @Override
      public Enumeration<String> getKeys() {
        return null;
      }

      @Override
      protected Object handleGetObject(String key) {
        return BaseMessages.getString(RepositoryExplorer.class, key);
      }
    };
    
    public void init(Repository rep) throws ControllerInitializationException {
      try {
        bindingFactory = new DefaultBindingFactory();
        bindingFactory.setDocument(getXulDomContainer().getDocumentRoot());
        
        XulEventHandler eventHandler = getXulDomContainer().getEventHandler("browseController"); //$NON-NLS-1$
        
        if(eventHandler instanceof BrowseController) {
          browseController = (BrowseController)eventHandler;
        }
        
        createBindings();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    
    private BindingConvertor<List<UIRepositoryObject>, Boolean> forButtons = new BindingConvertor<List<UIRepositoryObject>, Boolean>() {

      @Override
      public Boolean sourceToTarget(List<UIRepositoryObject> value) {
        return value != null && value.size() == 1;
      }

      @Override
      public List<UIRepositoryObject> targetToSource(Boolean value) {
        return null;
      }
    };
    
    public String getName() {
      return "repositoryLockController"; //$NON-NLS-1$
    }
    
    private BindingConvertor<List<UIRepositoryObject>, Boolean> checkLockedStateBool = new BindingConvertor<List<UIRepositoryObject>, Boolean>() {

      @Override
      public Boolean sourceToTarget(List<UIRepositoryObject> value) {
        boolean result = false;
        
        try {
          if(value != null && value.size() == 1 && value.get(0) != null) {
            if(value.get(0) instanceof UIRepositoryContent) {
              result = ((UIRepositoryContent)value.get(0)).isLocked();
            }
          }
        } catch(KettleException e) {
          throw new RuntimeException(e);
        }
        
        return result;
      }

      @Override
      public List<UIRepositoryObject> targetToSource(Boolean value) {
        return null;
      }
    };
    
    // This needs to exist until we have better method override support in the DefaultBinding
    private BindingConvertor<List<UIRepositoryObject>, String> checkLockedStateString = new BindingConvertor<List<UIRepositoryObject>, String>() {

      @Override
      public String sourceToTarget(List<UIRepositoryObject> value) {
        boolean result = false;
        
        try {
          if(value != null && value.size() == 1 && value.get(0) != null) {
            if(value.get(0) instanceof UIRepositoryContent) {
              result = ((UIRepositoryContent)value.get(0)).isLocked();
            }
          }
        } catch(KettleException e) {
          throw new RuntimeException(e);
        }
        
        return Boolean.toString(result);
      }

      @Override
      public List<UIRepositoryObject> targetToSource(String value) {
        return null;
      }
    };
    
    protected void createBindings() {
      // Lock bindings
      bindingFactory.createBinding(browseController, "repositoryObjects", "lock-menu", "!disabled", forButtons); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
      
      bindingFactory.createBinding(browseController, "repositoryObjects", "file-context-lock", "selected", checkLockedStateString); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
      bindingFactory.createBinding(browseController, "repositoryObjects", "file-context-locknotes", "!disabled", checkLockedStateBool); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
      
      bindingFactory.createBinding(browseController, "repositoryObjects", "lock-context-lock", "selected", checkLockedStateString); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
      bindingFactory.createBinding(browseController, "repositoryObjects", "lock-context-locknotes", "!disabled", checkLockedStateBool); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    public void lockContent() throws Exception {
      List<UIRepositoryObject> selectedRepoObjects = browseController.getSelectedFileItems();
            
      if(selectedRepoObjects.size() > 0 && selectedRepoObjects.get(0) instanceof UIRepositoryContent) {
        final UIRepositoryContent contentToLock = (UIRepositoryContent)selectedRepoObjects.get(0);
        
        if(contentToLock.isLocked()) {
          // Unlock the item
          contentToLock.unlock();
        } else {
          // Lock the item
          XulPromptBox lockNotePrompt = RepositoryExplorer.promptLockMessage(document, messages, null);
          lockNotePrompt.addDialogCallback(new XulDialogCallback<String>() {
            public void onClose(XulComponent component, Status status, String value) {
      
              if(!status.equals(Status.CANCEL)) {
                try {
                  contentToLock.lock(value);
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
      }
      }
    }
    
    public void viewLockNote() throws Exception {
      List<UIRepositoryObject> selectedRepoObjects = browseController.getSelectedFileItems();
      if(selectedRepoObjects.size() > 0 && selectedRepoObjects.get(0) instanceof UIRepositoryContent) {
        final UIRepositoryContent contentToLock = (UIRepositoryContent) selectedRepoObjects.get(0);
        System.out.println(contentToLock.getLockMessage());
      }
    }
  };
  
  @Override
  protected void setup() {
    RepositoryLockController repositoryLockController = new RepositoryLockController();
    
    overlays.add(new DefaultXulOverlay("org/pentaho/di/ui/repository/pur/xul/repository-lock-overlay.xul")); //$NON-NLS-1$
    controllerNames.add(repositoryLockController.getName());
    handlers.add(repositoryLockController);
  }

}
