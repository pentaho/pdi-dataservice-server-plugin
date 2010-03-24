package org.pentaho.di.ui.repository.repositoryexplorer.trash;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.ResourceBundle;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ITrashService;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.RepositoryElement;
import org.pentaho.di.repository.RepositoryObject;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.ui.repository.repositoryexplorer.ControllerInitializationException;
import org.pentaho.di.ui.repository.repositoryexplorer.IUIEEUser;
import org.pentaho.di.ui.repository.repositoryexplorer.RepositoryExplorer;
import org.pentaho.di.ui.repository.repositoryexplorer.controllers.BrowseController;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIJob;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryDirectories;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryDirectory;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryObject;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryObjects;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UITransformation;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingConvertor;
import org.pentaho.ui.xul.components.XulButton;
import org.pentaho.ui.xul.components.XulMessageBox;
import org.pentaho.ui.xul.containers.XulDeck;
import org.pentaho.ui.xul.containers.XulTree;

public class TrashBrowseController extends BrowseController {

  // ~ Static fields/initializers ======================================================================================

  // ~ Instance fields =================================================================================================

  private ResourceBundle messages = new ResourceBundle() {

    @Override
    public Enumeration<String> getKeys() {
      return null;
    }

    @Override
    protected Object handleGetObject(String key) {
      return BaseMessages.getString(IUIEEUser.class, key);
    }
    
  };  
  
  protected XulTree trashFileTable;

  protected XulDeck deck;

  protected List<UIRepositoryObject> selectedTrashFileItems;

  protected TrashDirectory trashDir = new TrashDirectory();

  protected ITrashService trashService;

  protected List<RepositoryElement> trash;

  protected Repository repository;
  
  protected XulButton undeleteButton;
  
  protected XulButton deleteButton;
  
  protected XulMessageBox messageBox;
  // ~ Constructors ====================================================================================================

  public TrashBrowseController() {
    super();
  }

  // ~ Methods =========================================================================================================

  /**
   * Intercept the repositoryDirectory.children and add the Trash directory to the end.
   */
  @Override
  protected Binding createDirectoryBinding() {
    bf.setBindingType(Binding.Type.ONE_WAY);
    return bf.createBinding(this, "repositoryDirectory", folderTree, "elements", //$NON-NLS-1$//$NON-NLS-2$
        new BindingConvertor<UIRepositoryDirectory, UIRepositoryDirectory>() {

          @Override
          public UIRepositoryDirectory sourceToTarget(final UIRepositoryDirectory value) {
            if (value == null || value.size() == 0) {
              return null;
            }
            if (!value.get(value.size() - 1).equals(trashDir)) {
              // add directly to children collection to bypass events
              value.getChildren().add(trashDir);
            }
            return value;
          }

          @Override
          public UIRepositoryDirectory targetToSource(final UIRepositoryDirectory value) {
            // not used
            return value;
          }

        });
  }

  protected class TrashDirectory extends UIRepositoryDirectory {

    @Override
    public String getImage() {
      return "images/trash.png"; //$NON-NLS-1$
    }

    @Override
    public String getName() {
      return messages.getString("trash"); //$NON-NLS-1$
    }

    @Override
    public UIRepositoryDirectories getChildren() {
      return new UIRepositoryDirectories();
    }

  }

  @Override
  public void init(Repository repository) throws ControllerInitializationException {
    super.init(repository);
    this.repository = repository;
    createBindings();
    try {
      messageBox = (XulMessageBox) document.createElement("messagebox");//$NON-NLS-1$
      trashService = (ITrashService) repository.getService(ITrashService.class);
    } catch (Throwable e) {
      throw new ControllerInitializationException(e);
    }
  }

  protected void doCreateBindings() {
    deck = (XulDeck) document.getElementById("browse-tab-right-panel-deck");//$NON-NLS-1$
    trashFileTable = (XulTree) document.getElementById("deleted-file-table"); //$NON-NLS-1$

    deleteButton = (XulButton) document.getElementById("delete-button"); //$NON-NLS-1$
    undeleteButton = (XulButton) document.getElementById("undelete-button"); //$NON-NLS-1$
    
    bf.setBindingType(Binding.Type.ONE_WAY);
    BindingConvertor<List<UIRepositoryObject>, Boolean> buttonConverter = new BindingConvertor<List<UIRepositoryObject>, Boolean>() {

      @Override
      public Boolean sourceToTarget(List<UIRepositoryObject> value) {
        if (value != null && value.size() > 0) {
          return true;
        }
        return false;
      }

      @Override
      public List<UIRepositoryObject> targetToSource(Boolean value) {
        // TODO Auto-generated method stub
        return null;
      }
    };
    bf.createBinding(trashFileTable, "selectedItems", this, "selectedTrashFileItems"); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(trashFileTable, "selectedItems", deleteButton, "!disabled", buttonConverter); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(trashFileTable, "selectedItems", undeleteButton, "!disabled", buttonConverter); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(trashFileTable, "selectedItems", "trash-context-delete", "!disabled", buttonConverter); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
    bf.createBinding(trashFileTable, "selectedItems", "trash-context-restore", "!disabled", buttonConverter); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    
    bf.setBindingType(Binding.Type.ONE_WAY);
    bf.createBinding(this, "trash", trashFileTable, "elements", //$NON-NLS-1$  //$NON-NLS-2$
        new BindingConvertor<List<RepositoryElement>, UIRepositoryObjects>() {
          @Override
          public UIRepositoryObjects sourceToTarget(List<RepositoryElement> trash) {
            UIRepositoryObjects listOfObjects = new UIRepositoryObjects();

            for (RepositoryElement elem : trash) {
              if (elem instanceof RepositoryDirectory) {
                RepositoryDirectory dir = (RepositoryDirectory) elem;
                // TODO fetch parent dir from somewhere
                listOfObjects.add(new UIRepositoryDirectory(dir, dirMap.get(dir.getParent() != null ? dir.getParent()
                    .getObjectId() : null), repository));
              } else {
                RepositoryObject c = (RepositoryObject) elem;
                if (c.getObjectType() == RepositoryObjectType.JOB) {
                  listOfObjects.add(new UIJob(c, dirMap.get(c.getRepositoryDirectory().getObjectId()), repository));
                } else {
                  listOfObjects.add(new UITransformation(c, dirMap.get(c.getRepositoryDirectory().getObjectId()),
                      repository));
                }
              }
            }
            return listOfObjects;
          }

          @Override
          public List<RepositoryElement> targetToSource(UIRepositoryObjects elements) {
            return null;
          }
        });
  }

  @Override
  public void setSelectedFolderItems(List<UIRepositoryDirectory> selectedFolderItems) {
    if (selectedFolderItems != null && selectedFolderItems.size() == 1 && selectedFolderItems.get(0).equals(trashDir)) {
      try {
        setTrash(trashService.getTrash());
      } catch (KettleException e) {
        throw new RuntimeException(e);
      }
      deck.setSelectedIndex(1);
    } else {
      deck.setSelectedIndex(0);
      super.setSelectedFolderItems(selectedFolderItems);
    }
  }

  public void setTrash(List<RepositoryElement> trash) {
    this.trash = trash;
    firePropertyChange("trash", null, trash); //$NON-NLS-1$
  }

  public List<RepositoryElement> getTrash() {
    return trash;
  }

  public void delete() {
    if (selectedTrashFileItems != null && selectedTrashFileItems.size() > 0) {
      List<ObjectId> ids = new ArrayList<ObjectId>();
      for (UIRepositoryObject uiObj : selectedTrashFileItems) {
        ids.add(uiObj.getObjectId());
      }
      try {
        trashService.delete(ids);
        setTrash(trashService.getTrash());
      } catch(Throwable th) {
        messageBox.setTitle(messages.getString("Dialog.Error"));//$NON-NLS-1$
        messageBox.setAcceptLabel(messages.getString("Dialog.Ok"));//$NON-NLS-1$
        messageBox.setMessage(BaseMessages.getString(IUIEEUser.class,
            "TrashBrowseController.UnableToDeleteFile", th.getLocalizedMessage())); //$NON-NLS-1$
        messageBox.open();
      }
    } else {
      // ui probably allowed the button to be enabled when it shouldn't have been enabled
      throw new RuntimeException();
    }
  }

  public void undelete(){
    // make a copy because the selected trash items changes as soon as trashService.undelete is called
    List<UIRepositoryObject> selectedTrashFileItemsSnapshot = new ArrayList<UIRepositoryObject>(selectedTrashFileItems);
    if (selectedTrashFileItemsSnapshot != null && selectedTrashFileItemsSnapshot.size() > 0) {
      List<ObjectId> ids = new ArrayList<ObjectId>();
      for (UIRepositoryObject uiObj : selectedTrashFileItemsSnapshot) {
        ids.add(uiObj.getObjectId());
      }
      try {
        trashService.undelete(ids);
        setTrash(trashService.getTrash());
        for (UIRepositoryObject uiObj : selectedTrashFileItemsSnapshot) {
          if (uiObj instanceof UIRepositoryDirectory) {
            // refresh the whole tree since XUL cannot refresh a portion of the tree at this time
            ((UIRepositoryDirectory) uiObj).refresh();
          } else {
            // refresh the files in the folder but only the affected folders
            uiObj.getParent().getRepositoryObjects().add(uiObj);
          }
        }        
      } catch(Throwable th) {
        messageBox.setTitle(messages.getString("Dialog.Error"));//$NON-NLS-1$
        messageBox.setAcceptLabel(messages.getString("Dialog.Ok"));//$NON-NLS-1$
        messageBox.setMessage(BaseMessages.getString(IUIEEUser.class,
            "TrashBrowseController.UnableToRestoreFile", th.getLocalizedMessage())); //$NON-NLS-1$
        messageBox.open();
      }
    } else {
      // ui probably allowed the button to be enabled when it shouldn't have been enabled
      throw new RuntimeException();
    }
  }

  public void setSelectedTrashFileItems(List<UIRepositoryObject> selectedTrashFileItems) {
    this.selectedTrashFileItems = selectedTrashFileItems;
  }

}
