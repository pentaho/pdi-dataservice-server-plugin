/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.ui.repository.pur.repositoryexplorer.controller;

import java.util.*;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.ILockObject;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.IRevisionObject;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.IUIEEUser;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.model.UIRepositoryObjectRevision;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.model.UIRepositoryObjectRevisions;
import org.pentaho.di.ui.repository.repositoryexplorer.ControllerInitializationException;
import org.pentaho.di.ui.repository.repositoryexplorer.IUISupportController;
import org.pentaho.di.ui.repository.repositoryexplorer.RepositoryExplorerCallback;
import org.pentaho.di.ui.repository.repositoryexplorer.controllers.BrowseController;
import org.pentaho.di.ui.repository.repositoryexplorer.controllers.MainController;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryContent;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryDirectory;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryObject;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingConvertor;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.binding.DefaultBindingFactory;
import org.pentaho.ui.xul.components.XulMessageBox;
import org.pentaho.ui.xul.components.XulPromptBox;
import org.pentaho.ui.xul.components.XulTab;
import org.pentaho.ui.xul.containers.XulTabbox;
import org.pentaho.ui.xul.containers.XulTree;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;
import org.pentaho.ui.xul.swt.custom.DialogConstant;
import org.pentaho.ui.xul.util.XulDialogCallback;

public class RevisionController  extends AbstractXulEventHandler implements IUISupportController, java.io.Serializable {

  private static final long serialVersionUID = 4072800373062217146L; /* EESOURCE: UPDATE SERIALVERUID */

  private static final Class<?> PKG = IUIEEUser.class;

  protected XulTree folderTree;
  protected XulTree fileTable;
  protected XulTab historyTab;
  protected Repository repository;
  protected XulTree revisionTable;
  protected MainController mainController;
  protected BrowseController browseController;
  protected BindingFactory bf;
  protected XulTabbox  filePropertiesTabbox;
  protected RepositoryExplorerCallback callback;
  protected UIRepositoryObjectRevisions revisions;
  protected Binding revisionBinding;

  private XulMessageBox messageBox;

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

  public RevisionController() {
    super();
    // TODO Auto-generated constructor stub
  }
  
  public void init(Repository repository) throws ControllerInitializationException {
    try {
      this.repository = repository; 
      mainController = (MainController) this.getXulDomContainer().getEventHandler("mainController"); //$NON-NLS-1$
      browseController = (BrowseController) this.getXulDomContainer().getEventHandler("browseController"); //$NON-NLS-1$
      bf = new DefaultBindingFactory();
      bf.setDocument(this.getXulDomContainer().getDocumentRoot());

      messageBox = (XulMessageBox) document.createElement("messagebox");//$NON-NLS-1$
      createBindings();
    } catch (Exception e) {
      throw new ControllerInitializationException(e);
    }
  }
 
  private void createBindings() {
    historyTab = (XulTab) document.getElementById("history"); //$NON-NLS-1$
    filePropertiesTabbox = (XulTabbox) document.getElementById("file-properties-tabs"); //$NON-NLS-1$
    revisionTable = (XulTree) document.getElementById("revision-table"); //$NON-NLS-1$
    folderTree = (XulTree) document.getElementById("folder-tree"); //$NON-NLS-1$
    fileTable = (XulTree) document.getElementById("file-table"); //$NON-NLS-1$ 

    bf.setBindingType(Binding.Type.ONE_WAY);
    BindingConvertor<int[], Boolean> forButtons = new BindingConvertor<int[], Boolean>() {

      @Override
      public Boolean sourceToTarget(int[] value) {
        return value != null && !(value.length <= 0);
      }

      @Override
      public int[] targetToSource(Boolean value) {
        return null;
      }
    };

    Binding buttonBinding = bf.createBinding(revisionTable, "selectedRows", "revision-open", "!disabled", forButtons); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    bf.setBindingType(Binding.Type.ONE_WAY);
    bf.createBinding(folderTree, "selectedItems", this, "historyTabVisibility"); //$NON-NLS-1$  //$NON-NLS-2$

    revisionBinding = bf.createBinding(this, "revisionObjects", revisionTable, "elements");//$NON-NLS-1$ //$NON-NLS-2$
        
    revisionBinding = bf.createBinding(browseController, "repositoryItems",  this, "revisionObjects",//$NON-NLS-1$ //$NON-NLS-2$
        new BindingConvertor<List<UIRepositoryObject>, UIRepositoryObjectRevisions>() {
          @Override
          public UIRepositoryObjectRevisions sourceToTarget(List<UIRepositoryObject> ro) {
            UIRepositoryObjectRevisions revisions = new UIRepositoryObjectRevisions();

            if (ro == null) {
              return new UIRepositoryObjectRevisions();
            }
            if (ro.size() <= 0) {
              return new UIRepositoryObjectRevisions();
            }
            if (ro.get(0) instanceof UIRepositoryDirectory) {
              historyTab.setVisible(false);
              filePropertiesTabbox.setSelectedIndex(0);
              return null;
            }
            try {
              UIRepositoryContent rc = (UIRepositoryContent) ro.get(0);
              if(rc instanceof IRevisionObject) {
                revisions = ((IRevisionObject)rc).getRevisions();
              } else {
                throw new IllegalStateException(BaseMessages.getString(PKG, "RevisionsController.RevisionsNotSupported")); //$NON-NLS-1$
              }
            } catch (KettleException e) {
              // convert to runtime exception so it bubbles up through the UI
              throw new RuntimeException(e);
            }
            historyTab.setVisible(true);
            return revisions;
          }

          @Override
          public List<UIRepositoryObject> targetToSource(UIRepositoryObjectRevisions elements) {
            return null;
          }
        });

    try {
      buttonBinding.fireSourceChanged();
      revisionBinding.fireSourceChanged();
    } catch (Exception e) {
      // convert to runtime exception so it bubbles up through the UI
      throw new RuntimeException(e);
    }

  }
  
  public void setRevisionObjects(UIRepositoryObjectRevisions revisions){
    this.revisions = revisions;
    this.firePropertyChange("revisionObjects", null, revisions);
  }
  
  public UIRepositoryObjectRevisions getRevisionObjects(){
    return revisions;
  }
  
  public String getName() {
    return "revisionController"; //$NON-NLS-1$
  }

  public <T> void setHistoryTabVisibility(Collection<T> items) {
      historyTab.setVisible(false);
      filePropertiesTabbox.setSelectedIndex(0);
  }


  public RepositoryExplorerCallback getCallback() {
    return callback;
  }

  public void setCallback(RepositoryExplorerCallback callback) {
    this.callback = callback;
  }
  
  public void openRevision() {
    Collection<UIRepositoryContent> content = fileTable.getSelectedItems();
    UIRepositoryContent contentToOpen = content.iterator().next();

    Collection<UIRepositoryObjectRevision> revision = revisionTable.getSelectedItems();

    // TODO: Is it a requirement to allow opening multiple revisions? 
    UIRepositoryObjectRevision revisionToOpen = revision.iterator().next();
    if (mainController != null && mainController.getCallback() != null) {
      if (mainController.getCallback().open(contentToOpen, revisionToOpen.getName())) {
        //TODO: fire request to close dialog
      }
    }
  }

  public void restoreRevision() {
    
    try {
      final Collection<UIRepositoryContent> content = fileTable.getSelectedItems();
      final UIRepositoryContent contentToRestore = content.iterator().next();

      Collection<UIRepositoryObjectRevision> versions = revisionTable.getSelectedItems();
      final UIRepositoryObjectRevision versionToRestore = versions.iterator().next();
      
      XulPromptBox commitPrompt = promptCommitComment(document, messages, null);

      if (((ILockObject)contentToRestore).isLocked()) {
        // Cannot restore revision of locked content
        messageBox.setTitle(BaseMessages.getString(PKG, "Dialog.Error"));//$NON-NLS-1$
        messageBox.setAcceptLabel(BaseMessages.getString(PKG, "Dialog.Ok"));//$NON-NLS-1$
        messageBox.setMessage(BaseMessages.getString(PKG, "RevisionsController.RestoreLockedFileNotAllowed")); //$NON-NLS-1$
        messageBox.open();
        return;
      }

      commitPrompt.addDialogCallback(new XulDialogCallback<String>() {
        public void onClose(XulComponent component, Status status, String value) {

          if (!status.equals(Status.CANCEL)) {
            try {
              if(contentToRestore instanceof IRevisionObject) {
                ((IRevisionObject)contentToRestore).restoreRevision(versionToRestore, value);
                List<UIRepositoryObject> objects = new ArrayList<UIRepositoryObject>();
                objects.add(contentToRestore);
                browseController.setRepositoryObjects(objects);    
              } else {
                throw new IllegalStateException(BaseMessages.getString(PKG, "RevisionsController.RevisionsNotSupported")); //$NON-NLS-1$
              }
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

      commitPrompt.open();
    } catch (Exception e) {
      throw new RuntimeException(new KettleException(e));
    }
  }
  

  private XulPromptBox promptCommitComment(org.pentaho.ui.xul.dom.Document document, ResourceBundle messages,
      String defaultMessage) throws XulException {
    XulPromptBox prompt = (XulPromptBox) document.createElement("promptbox"); //$NON-NLS-1$

    prompt.setTitle(BaseMessages.getString(PKG, "RepositoryExplorer.CommitTitle"));//$NON-NLS-1$
    prompt.setButtons(new DialogConstant[] { DialogConstant.OK, DialogConstant.CANCEL });

    prompt.setMessage(BaseMessages.getString(PKG, "RepositoryExplorer.CommitLabel"));//$NON-NLS-1$
    prompt
        .setValue(defaultMessage == null ? BaseMessages.getString(PKG, "RepositoryExplorer.DefaultCommitMessage") : defaultMessage); //$NON-NLS-1$
    return prompt;
  }
}
