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

package org.pentaho.di.ui.repository.pur.repositoryexplorer.controller;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.ResourceBundle;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.repository.pur.RepositoryObjectAccessException;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.IUIEEUser;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.model.UIEERepositoryDirectory;
import org.pentaho.di.ui.repository.pur.services.ITrashService;
import org.pentaho.di.ui.repository.pur.services.ITrashService.IDeletedObject;
import org.pentaho.di.ui.repository.repositoryexplorer.ControllerInitializationException;
import org.pentaho.di.ui.repository.repositoryexplorer.controllers.BrowseController;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryDirectories;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryDirectory;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryObject;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryObjects;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.XulEventSourceAdapter;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingConvertor;
import org.pentaho.ui.xul.components.XulButton;
import org.pentaho.ui.xul.components.XulConfirmBox;
import org.pentaho.ui.xul.components.XulPromptBox;
import org.pentaho.ui.xul.containers.XulDeck;
import org.pentaho.ui.xul.containers.XulTree;
import org.pentaho.ui.xul.util.XulDialogCallback;

public class TrashBrowseController extends BrowseController implements java.io.Serializable {

  private static final long serialVersionUID = -3822571463115111325L; /* EESOURCE: UPDATE SERIALVERUID */

  // ~ Static fields/initializers ======================================================================================

  private static final Class<?> PKG = IUIEEUser.class;

  // ~ Instance fields =================================================================================================

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
  
  protected XulTree trashFileTable;

  protected XulDeck deck;

  protected List<UIDeletedObject> selectedTrashFileItems;

  protected TrashDirectory trashDir = new TrashDirectory();

  protected ITrashService trashService;

  protected List<IDeletedObject> trash;

  protected XulButton undeleteButton;
  
  protected XulButton deleteButton;
  
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

  protected class TrashDirectory extends UIEERepositoryDirectory {

    private static final long serialVersionUID = 6184312253116517468L;

    @Override
    public String getImage() {
      return "images/trash.png"; //$NON-NLS-1$
    }

    @Override
    public String getName() {
      return BaseMessages.getString(PKG, "Trash"); //$NON-NLS-1$
    }

    @Override
    public UIRepositoryDirectories getChildren() {
      return new UIRepositoryDirectories();
    }

    @Override
    public UIRepositoryObjects getRepositoryObjects() throws KettleException {
      return new UIRepositoryObjects();
    }
  }

  @Override
  public void init(Repository repository) throws ControllerInitializationException {
    super.init(repository);
    try {
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
    BindingConvertor<List<UIDeletedObject>, Boolean> buttonConverter = new BindingConvertor<List<UIDeletedObject>, Boolean>() {

      @Override
      public Boolean sourceToTarget(List<UIDeletedObject> value) {
        if (value != null && value.size() > 0) {
          return true;
        }
        return false;
      }

      @Override
      public List<UIDeletedObject> targetToSource(Boolean value) {
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
        new BindingConvertor<List<IDeletedObject>, List<UIDeletedObject>>() {
          @Override
          public List<UIDeletedObject> sourceToTarget(List<IDeletedObject> trash) {
            List<UIDeletedObject> newList = new ArrayList<UIDeletedObject>(trash.size());
            for (IDeletedObject obj : trash) {
              newList.add(new UIDeletedObject(obj));
            }
            Collections.sort(newList, new UIDeletedObjectComparator());
            return newList;
          }

          @Override
          public List<IDeletedObject> targetToSource(List<UIDeletedObject> elements) {
            return null;
          }
        });
  }

  /**
   * An IDeletedObject that is also a XulEventSource.
   */
  public static class UIDeletedObject extends XulEventSourceAdapter {
 
    private IDeletedObject obj;
    
    private static Comparator<UIDeletedObject> comparator = new UIDeletedObjectComparator();
    
    public UIDeletedObject(final IDeletedObject obj) {
      this.obj = obj;
    }
    
    public String getOriginalParentPath() {
      return obj.getOriginalParentPath();
    }

    public String getDeletedDate() {
      Date date = obj.getDeletedDate();
      String str = null;
      if (date != null){
        SimpleDateFormat sdf = new SimpleDateFormat("d MMM yyyy HH:mm:ss z"); //$NON-NLS-1$
        str = sdf.format(date);
      }
      return str;
    }

    public String getType() {
      return obj.getType();  
    }

    public ObjectId getId() {
      return obj.getId();  
    }

    public String getName() {
      return obj.getName();
    }
    
    public String getImage() {
      if (RepositoryObjectType.TRANSFORMATION.name().equals(obj.getType())) {
        return "images/transformation.png"; //$NON-NLS-1$
      } else if (RepositoryObjectType.JOB.name().equals(obj.getType())) {
        return "images/job.png"; //$NON-NLS-1$
      } else {
        return "images/treeClosed.png"; //$NON-NLS-1$
      }
    }
    
    public Comparator<UIDeletedObject> getComparator() {
      return comparator;
    }
    
  }
  
  public static class UIDeletedObjectComparator implements Comparator<UIDeletedObject> {

    public int compare(UIDeletedObject o1, UIDeletedObject o2) {
      int cat1 = getValue(o1.getType());
      int cat2 = getValue(o2.getType());
      if (cat1 != cat2) {
        return cat1 - cat2;
      }
      String t1 = o1.getName();
      String t2 = o2.getName();
      if (t1 == null) t1 = ""; //$NON-NLS-1$
      if (t2 == null) t2 = ""; //$NON-NLS-1$
      return t1.compareToIgnoreCase(t2);
    }
    
    private int getValue(final String type) {
      if (type == null) {
        return 10;
      } else {
        return 20;
      }
    }
    
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

  public void setTrash(List<IDeletedObject> trash) {
    this.trash = trash;
    firePropertyChange("trash", null, trash); //$NON-NLS-1$
  }

  public List<IDeletedObject> getTrash() {
    return trash;
  }

  @Override
  protected void moveFiles(List<UIRepositoryObject> objects, UIRepositoryDirectory targetDirectory) throws Exception {
    // If we're moving into the trash it's really a delete
    if (targetDirectory != trashDir) {
      super.moveFiles(objects, targetDirectory);
    } else {
      for (UIRepositoryObject o : objects) {
        deleteContent(o);
      }
    }
  }

  public void delete() {
    if (selectedTrashFileItems != null && selectedTrashFileItems.size() > 0) {
      List<ObjectId> ids = new ArrayList<ObjectId>();
      for (UIDeletedObject uiObj : selectedTrashFileItems) {
        ids.add(uiObj.getId());
      }
      try {
        trashService.delete(ids);
        setTrash(trashService.getTrash());
      } catch(Throwable th) {
        displayExceptionMessage(BaseMessages.getString(PKG,
            "TrashBrowseController.UnableToDeleteFile", th.getLocalizedMessage())); //$NON-NLS-1$
      }
    } else {
      // ui probably allowed the button to be enabled when it shouldn't have been enabled
      throw new RuntimeException();
    }
  }

  public void undelete(){
    // make a copy because the selected trash items changes as soon as trashService.undelete is called
    List<UIDeletedObject> selectedTrashFileItemsSnapshot = new ArrayList<UIDeletedObject>(selectedTrashFileItems);
    if (selectedTrashFileItemsSnapshot != null && selectedTrashFileItemsSnapshot.size() > 0) {
      List<ObjectId> ids = new ArrayList<ObjectId>();
      for (UIDeletedObject uiObj : selectedTrashFileItemsSnapshot) {
        ids.add(uiObj.getId());
      }
      try {
        trashService.undelete(ids);
        setTrash(trashService.getTrash());
        for (UIDeletedObject uiObj : selectedTrashFileItemsSnapshot) {
          // find the closest UIRepositoryDirectory that is in the dirMap
          RepositoryDirectoryInterface dir = repository.findDirectory(uiObj.getOriginalParentPath());
          while (dir != null && dirMap.get(dir.getObjectId()) == null) {
            dir = dir.getParent();
          }
          // now refresh that UIRepositoryDirectory so that the file/folders deck instantly refreshes on undelete
          if (dir != null) {
            dirMap.get(dir.getObjectId()).refresh();
          }
        }
        deck.setSelectedIndex(1);
      } catch(Throwable th) {
        displayExceptionMessage(BaseMessages.getString(PKG,
            "TrashBrowseController.UnableToRestoreFile", th.getLocalizedMessage())); //$NON-NLS-1$
      }
    } else {
      // ui probably allowed the button to be enabled when it shouldn't have been enabled
      throw new RuntimeException();
    }
  }

  public void setSelectedTrashFileItems(List<UIDeletedObject> selectedTrashFileItems) {
    this.selectedTrashFileItems = selectedTrashFileItems;
  }
  
  @Override
  protected void deleteFolder(UIRepositoryDirectory repoDir) throws Exception{
    deleteContent(repoDir);
  }
   
  @Override
  protected void deleteContent(final UIRepositoryObject repoObject) throws Exception {
    try {
      repoObject.delete();
    } catch (KettleException ke) { 
      if (ke.getCause() instanceof RepositoryObjectAccessException) {
        moveDeletePrompt(ke, repoObject, new XulDialogCallback<Object>() {
  
          public void onClose(XulComponent sender, Status returnCode, Object retVal) {
            if (returnCode == Status.ACCEPT) {
              try{
                ((UIEERepositoryDirectory)repoObject).delete(true);
              } catch (Exception e) {
                displayExceptionMessage(BaseMessages.getString(PKG, e.getLocalizedMessage()));
              }
            }
          }
  
          public void onError(XulComponent sender, Throwable t) {
            throw new RuntimeException(t);
          }
          
        });
      } else {
        throw ke;
      }
    }
    
    if (repoObject instanceof UIRepositoryDirectory) {
      directoryBinding.fireSourceChanged();
      if(repoDir != null) {
        repoDir.refresh();        
      }
    }
    selectedItemsBinding.fireSourceChanged();
  }
  
  @Override
  protected void renameRepositoryObject(final UIRepositoryObject repoObject) throws XulException {
    // final Document doc = document;
    XulPromptBox prompt = promptForName(repoObject);
    prompt.addDialogCallback(new XulDialogCallback<String>() {
      public void onClose(XulComponent component, Status status, String value) {
        if (status == Status.ACCEPT) {
          final String newName = value;
          try {
            repoObject.setName(newName);
          } catch (KettleException ke) {
            if (ke.getCause() instanceof RepositoryObjectAccessException) {
              moveDeletePrompt(ke, repoObject, new XulDialogCallback<Object>() {
  
                public void onClose(XulComponent sender, Status returnCode, Object retVal) {
                  if (returnCode == Status.ACCEPT) {
                    try{
                     ((UIEERepositoryDirectory)repoObject).setName(newName, true);
                    } catch (Exception e) {
                      displayExceptionMessage(BaseMessages.getString(PKG, e.getLocalizedMessage()));
                    }
                  }
                }
  
                public void onError(XulComponent sender, Throwable t) {
                  throw new RuntimeException(t);
                }
                
              });
            } else {
              throw new RuntimeException(ke);
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

    prompt.open();
  }
  
  
  
  protected boolean moveDeletePrompt(final KettleException ke, final UIRepositoryObject repoObject, final XulDialogCallback<Object> action) {
    if(ke.getCause() instanceof RepositoryObjectAccessException &&
        ((RepositoryObjectAccessException)ke.getCause()).getObjectAccessType().equals(RepositoryObjectAccessException.AccessExceptionType.USER_HOME_DIR) && 
        repoObject instanceof UIEERepositoryDirectory) {
        
      try {
        confirmBox = (XulConfirmBox) document.createElement("confirmbox");//$NON-NLS-1$
        confirmBox.setTitle(BaseMessages.getString(PKG, "TrashBrowseController.DeleteHomeFolderWarningTitle")); //$NON-NLS-1$
        confirmBox.setMessage(BaseMessages.getString(PKG, "TrashBrowseController.DeleteHomeFolderWarningMessage")); //$NON-NLS-1$
        confirmBox.setAcceptLabel(BaseMessages.getString(PKG, "Dialog.Ok")); //$NON-NLS-1$
        confirmBox.setCancelLabel(BaseMessages.getString(PKG, "Dialog.Cancel")); //$NON-NLS-1$
        confirmBox.addDialogCallback(new XulDialogCallback<Object>() {

          public void onClose(XulComponent sender, Status returnCode, Object retVal) {
            if (returnCode == Status.ACCEPT) {
              action.onClose(sender, returnCode, retVal);
            }
          }

          public void onError(XulComponent sender, Throwable t) {
            throw new RuntimeException(t);
          }
        });
        confirmBox.open();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return false;
  }
  
  protected void displayExceptionMessage(String msg) {
    messageBox.setTitle(BaseMessages.getString(PKG, "Dialog.Error")); //$NON-NLS-1$
    messageBox.setAcceptLabel(BaseMessages.getString(PKG, "Dialog.Ok")); //$NON-NLS-1$
    messageBox.setMessage(msg);
    messageBox.open();
  }

}
