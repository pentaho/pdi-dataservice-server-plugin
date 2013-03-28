package org.pentaho.di.repository.pur.metastore;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.pentaho.di.repository.pur.PurRepository;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreDependenciesExistsException;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.api.exceptions.MetaStoreNamespaceExistsException;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.repository2.ClientRepositoryPaths;

/**
 * Please note that for this class to work, the supplied PurRepository needs to be connected to the server.
 * 
 * @author matt
 *
 */
public class PurRepositoryMetaStore extends MemoryMetaStore implements IMetaStore {
  
  public static final String FOLDER_METASTORE = "metastore";
  
  protected PurRepository repository;
  protected IUnifiedRepository pur;

  private RepositoryFile namespacesFolder;
  
  public PurRepositoryMetaStore(PurRepository repository) {
    this.repository = repository;
    this.pur = repository.getPur();
    verifyEnvironment();
  }
  
  @Override
  public void createNamespace(String namespace) throws MetaStoreException, MetaStoreNamespaceExistsException {
    if (namespaceExists(namespace)) {
      throw new MetaStoreNamespaceExistsException("Namespace '"+namespace+"' can not be created, it already exists");
    }
    pur.createFolder(namespacesFolder.getId(), buildFolder(namespacesFolder.getPath(), namespace), "Created namespace");
  }
  
  @Override
  public boolean namespaceExists(String namespace) throws MetaStoreException {
    return pur.getFile(getNamespaceFolderPath(namespace))!=null;
  }
  
  @Override
  public void deleteNamespace(String namespace) throws MetaStoreException, MetaStoreDependenciesExistsException {
    RepositoryFile namespaceFile = pur.getFile(getNamespaceFolderPath(namespace));
    if (namespaceFile==null) {
      return; // already gone.
    }
    List<RepositoryFile> children = pur.getChildren(namespaceFile.getId());
    if (children==null || children.isEmpty()) {
      // Delete the file, there are no children.
      //
      pur.deleteFile(namespaceFile.getId(), "Delete namespace");
    } else {
      // Dependencies exists, throw an exception.
      //
      List<String> elementTypeIds = new ArrayList<String>();
      for (RepositoryFile child : children) {
        elementTypeIds.add(child.getId().toString());
      }
      throw new MetaStoreDependenciesExistsException(elementTypeIds, "Namespace '"+namespace+" can not be deleted because it is not empty");
    }
  }
  
  // TODO: complete implementation of element types and elements.
  //
  
  
  
  
  

  protected String getNamespaceFolderPath(String namespace) {
    return getMetaStoreParentFolderPath()+RepositoryFile.SEPARATOR+namespace;
  }

  /**
   * Verify that /etc/metastore exists...
   * 
   */
  protected void verifyEnvironment() {
    
    RepositoryFile etcFolder = pur.getFile(ClientRepositoryPaths.getEtcFolderPath());
    
    namespacesFolder = pur.getFile(getMetaStoreParentFolderPath());
    if (namespacesFolder==null) {
      namespacesFolder = pur.createFolder(etcFolder.getId(), buildFolder(etcFolder.getPath(), FOLDER_METASTORE), "Automatic creation by PUR metastore client");
    }
  }
  
  protected RepositoryFile buildFolder(String path, String foldername) {
    return new RepositoryFile(null, foldername, false, false, false, null, path, new Date(), new Date(), false, null, null, null, null, null, null, null, null, 0L, null, null);  
  }


  private String getMetaStoreParentFolderPath() {
    return ClientRepositoryPaths.getEtcFolderPath() + RepositoryFile.SEPARATOR + FOLDER_METASTORE;
  }
  
}
