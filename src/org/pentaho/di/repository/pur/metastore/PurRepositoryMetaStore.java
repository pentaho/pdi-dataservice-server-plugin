package org.pentaho.di.repository.pur.metastore;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.pur.PurRepository;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.IMetaStoreAttribute;
import org.pentaho.metastore.api.IMetaStoreElement;
import org.pentaho.metastore.api.IMetaStoreElementType;
import org.pentaho.metastore.api.exceptions.MetaStoreDependenciesExistsException;
import org.pentaho.metastore.api.exceptions.MetaStoreElementExistException;
import org.pentaho.metastore.api.exceptions.MetaStoreElementTypeExistsException;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.api.exceptions.MetaStoreNamespaceExistsException;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.api.repository2.unified.RepositoryFileAcl;
import org.pentaho.platform.api.repository2.unified.data.node.DataNode;
import org.pentaho.platform.api.repository2.unified.data.node.DataProperty;
import org.pentaho.platform.api.repository2.unified.data.node.NodeRepositoryFileData;
import org.pentaho.platform.repository2.ClientRepositoryPaths;

/**
 * Please note that for this class to work, the supplied PurRepository needs to be connected to the server.
 * 
 * @author matt
 *
 */
public class PurRepositoryMetaStore extends MemoryMetaStore implements IMetaStore {
  
  public static final String FOLDER_METASTORE = "metastore";
  
  public static final String ELEMENT_TYPE_DETAILS_FILENAME = "ElementTypeDetails";
  
  protected static final String PROP_NAME = "NAME"; //$NON-NLS-1$
  
  protected static final String PROP_ELEMENT_TYPE_NAME = "element_type_name"; //$NON-NLS-1$

  protected static final String PROP_ELEMENT_TYPE_DESCRIPTION = "element_type_description"; //$NON-NLS-1$

  private static final String METASTORE_FOLDER_PATH = "/etc/metastore";
  
  protected PurRepository repository;
  protected IUnifiedRepository pur;

  /**
   * This is the folder where the namespaces folders are stored
   */
  protected RepositoryFile namespacesFolder;
  
  public PurRepositoryMetaStore(PurRepository repository) throws KettleException {
    this.repository = repository;
    this.pur = repository.getPur();
    
    namespacesFolder = pur.getFile(METASTORE_FOLDER_PATH);
    if (namespacesFolder==null) {
      throw new KettleException(METASTORE_FOLDER_PATH+" folder is not available"); 
    }
  }
  
  @Override
  public String getName() {
    return repository.getRepositoryMeta().getName();
  }
  
  @Override
  public String getDescription() {
    return repository.getRepositoryMeta().getDescription();
  }
  
  // The namespaces
  
  
  @Override
  public void createNamespace(String namespace) throws MetaStoreException, MetaStoreNamespaceExistsException {
    if (namespaceExists(namespace)) {
      throw new MetaStoreNamespaceExistsException("Namespace '"+namespace+"' can not be created, it already exists");
    }
    pur.createFolder(namespacesFolder.getId(), buildFolder(namespacesFolder.getPath(), namespace), "Created namespace");
  }
  
  @Override
  public boolean namespaceExists(String namespace) throws MetaStoreException {
    return getNamespaceRepositoryFile(namespace)!=null;
  }
  
  
  @Override
  public void deleteNamespace(String namespace) throws MetaStoreException, MetaStoreDependenciesExistsException {
    RepositoryFile namespaceFile = getNamespaceRepositoryFile(namespace);
    if (namespaceFile==null) {
      return; // already gone.
    }
    List<RepositoryFile> children = pur.getChildren(namespaceFile.getId());
    if (children==null || children.isEmpty()) {
      // Delete the file, there are no children.
      //
      pur.deleteFile(namespaceFile.getId(), true, "Delete namespace");
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
  
  @Override
  public List<String> getNamespaces() throws MetaStoreException {
    List<String> namespaces = new ArrayList<String>();
    List<RepositoryFile> children = pur.getChildren(namespacesFolder.getId());
    for (RepositoryFile child : children) {
      namespaces.add(child.getName());
    }
    return namespaces;
  }
  
  
  // The element types
  
  @Override
  public void createElementType(String namespace, IMetaStoreElementType elementType) throws MetaStoreException,
      MetaStoreElementTypeExistsException {

    IMetaStoreElementType existingType = getElementTypeByName(namespace, elementType.getName());
    if (existingType!=null) {
      throw new MetaStoreElementTypeExistsException(Arrays.asList(existingType), "Can not create element type with id '"+elementType.getId()+"' because it already exists");
    }
    RepositoryFile namespaceFile = validateNamespace(namespace);
    RepositoryFile elementTypeFile = new RepositoryFile.Builder(elementType.getName())
      .folder(true)
      .build();
    
    RepositoryFile folder = pur.createFolder(namespaceFile.getId(), elementTypeFile, null);
    elementType.setId(folder.getId().toString());
    
    // In this folder there is a hidden file which contains the description 
    // and the other future properties of the element type
    //
    RepositoryFile detailsFile = new RepositoryFile.Builder(ELEMENT_TYPE_DETAILS_FILENAME)
      .folder(false)
      .description(elementType.getDescription())
      .hidden(true)
      .build();
    
    DataNode dataNode = new DataNode(ELEMENT_TYPE_DETAILS_FILENAME);
    dataNode.setProperty(PROP_ELEMENT_TYPE_DESCRIPTION, elementType.getDescription());
    dataNode.setProperty(PROP_ELEMENT_TYPE_NAME, elementType.getName());
    dataNode.setProperty(PROP_NAME, elementType.getName());
    
    pur.createFile(folder.getId(), detailsFile, new NodeRepositoryFileData(dataNode), null);
  }
  
  @Override
  public IMetaStoreElementType getElementType(String namespace, String elementTypeId) throws MetaStoreException {
    RepositoryFile elementTypeFolder = getElementTypeRepositoryFolder(elementTypeId);
    if (elementTypeFolder==null) {
      return null;
    }
    IMetaStoreElementType elementType = newElementType(namespace);
    elementType.setId(elementTypeFolder.getId().toString());
    elementType.setName(elementTypeFolder.getName());
    
    RepositoryFile detailsFile = findChild(elementTypeFolder.getId(), ELEMENT_TYPE_DETAILS_FILENAME);
    if (detailsFile!=null) {
      NodeRepositoryFileData data = pur.getDataForRead(detailsFile.getId(), NodeRepositoryFileData.class);
      DataProperty property = data.getNode().getProperty("element_type_description");
      if (property!=null) {
        elementType.setDescription(property.getString());
      }
    }
    return elementType;
  }
  
  @Override
  public List<IMetaStoreElementType> getElementTypes(String namespace) throws MetaStoreException {
    List<IMetaStoreElementType> elementTypes = new ArrayList<IMetaStoreElementType>();
    
    RepositoryFile namespaceFile = validateNamespace(namespace);
    List<RepositoryFile> children = pur.getChildren(namespaceFile.getId());
    for (RepositoryFile child : children) {
      elementTypes.add(getElementType(namespace, child.getId().toString()));
    }
    
    return elementTypes;
  }
  
  @Override
  public IMetaStoreElementType getElementTypeByName(String namespace, String elementTypeName) throws MetaStoreException {
    RepositoryFile file = getElementTypeRepositoryFileByName(namespace, elementTypeName);
    if (file==null) {
      return null;
    }
    
    return getElementType(namespace, file.getId().toString());
  }
  
  @Override
  public List<String> getElementTypeIds(String namespace) throws MetaStoreException {
    List<String> ids = new ArrayList<String>();
    
    for (IMetaStoreElementType type : getElementTypes(namespace)) {
      ids.add(type.getId());
    }
    
    return ids;
  }
  
  @Override
  public void deleteElementType(String namespace, String elementTypeId) throws MetaStoreException,
      MetaStoreDependenciesExistsException {
    
    validateNamespace(namespace);
    
    RepositoryFile elementTypeFile = pur.getFileById(elementTypeId);
    List<RepositoryFile> children = pur.getChildren(elementTypeFile.getId());
    removeHiddenFilesFromList(children);
    
    if (children.isEmpty()) {
      pur.deleteFile(elementTypeFile.getId(), true, null);
    } else {
      List<String> ids = getElementIds(namespace, elementTypeId);
      throw new MetaStoreDependenciesExistsException(ids, "Can't delete element type with id '"+elementTypeId+"' because it is not empty");
    }
  }
  
  
  protected void removeHiddenFilesFromList(List<RepositoryFile> children) {
    
    for (Iterator<RepositoryFile> it=children.iterator();it.hasNext(); ) {
      RepositoryFile child = it.next();
      if (child.isHidden()) {
        it.remove();
      }
    }
    
  }

  // The elements
  
  public void createElement(String namespace, String elementTypeId, 
      IMetaStoreElement element) throws MetaStoreException, MetaStoreElementExistException {
    RepositoryFile elementTypeFolder = validateElementTypeRepositoryFolder(elementTypeId);
    
    RepositoryFile elementFile = new RepositoryFile.Builder(element.getName()).build();
    
    DataNode elementDataNode = new DataNode(element.getName());
    elementToDataNode(element, elementDataNode);
  
    RepositoryFile createdFile = pur.createFile(elementTypeFolder.getId(), elementFile, new NodeRepositoryFileData(elementDataNode), null);
    element.setId(createdFile.getId().toString());
  };
  

  @Override
  public IMetaStoreElement getElement(String namespace, String elementTypeId, String elementId)
      throws MetaStoreException {
    
    RepositoryFile elementFile = pur.getFileById(elementId);
    IMetaStoreElement element = newElement(getElementType(namespace, elementTypeId), elementId, null);
    element.setName(elementFile.getName());
    NodeRepositoryFileData data = pur.getDataForRead(elementId, NodeRepositoryFileData.class);
    dataNodeToAttribute(data.getNode(), element);
    
    return element;
  }
  
  @Override
  public List<IMetaStoreElement> getElements(String namespace, String elementTypeId) throws MetaStoreException {
    List<IMetaStoreElement> elements = new ArrayList<IMetaStoreElement>();
    
    RepositoryFile typeFolder = validateElementTypeRepositoryFolder(elementTypeId);
    List<RepositoryFile> children = pur.getChildren(typeFolder.getId());
    removeHiddenFilesFromList(children);
    for (RepositoryFile child : children) {
      IMetaStoreElement element = getElement(namespace, elementTypeId, child.getId().toString());
      elements.add(element);
    }
    
    return elements;
  }
  
  @Override
  public IMetaStoreElement getElementByName(String namespace, IMetaStoreElementType elementType, String name)
      throws MetaStoreException {
    for (IMetaStoreElement element : getElements(namespace, elementType.getId())) {
      if (element.getName().equals(name)) {
        return element;
      }
    }
    return null;
  }
  
  @Override
  public List<String> getElementIds(String namespace, String elementTypeId) throws MetaStoreException {
    RepositoryFile folder = validateElementTypeRepositoryFolder(elementTypeId);
    List<RepositoryFile> children = pur.getChildren(folder.getId());
    removeHiddenFilesFromList(children);
    List<String> ids = new ArrayList<String>();
    for (RepositoryFile child : children) {
      ids.add(child.getId().toString());
    }
    return ids;
  }
  
  @Override
  public void deleteElement(String namespace, String elementTypeId, String elementId) throws MetaStoreException {
    
    pur.deleteFile(elementId, true, null);
    
  }
  
  
  
  
  
  
  
  
  
  protected void elementToDataNode(IMetaStoreAttribute attribute, DataNode dataNode) {
    for (IMetaStoreAttribute child: attribute.getChildren()) {
      Object value = child.getValue();
      if (child.getChildren().isEmpty()) {
        if (value==null) {
          continue;
        }
        if (value instanceof Double) {
          dataNode.setProperty(child.getId(), (Double)value);
        } else if (value instanceof Date) {
          dataNode.setProperty(child.getId(), (Date)value);
        } else if (value instanceof Long) {
          dataNode.setProperty(child.getId(), (Long)value);
        } else {
          dataNode.setProperty(child.getId(), value.toString());
        }
      } else {
        DataNode subNode = new DataNode(child.getId());
        elementToDataNode(child, subNode);
        dataNode.addNode(subNode);
      }
    }
  }
  
  protected void dataNodeToAttribute(DataNode dataNode, IMetaStoreAttribute attribute) throws MetaStoreException {
    // First process the properties
    //
    Iterable<DataProperty> properties = dataNode.getProperties();
    for (Iterator<DataProperty> it=properties.iterator();it.hasNext();) {
      DataProperty property = it.next();
      switch(property.getType()) {
        case DATE : attribute.addChild(newAttribute(property.getName(), property.getDate())); break;
        case DOUBLE: attribute.addChild(newAttribute(property.getName(), property.getDouble())); break;
        case STRING: attribute.addChild(newAttribute(property.getName(), property.getString())); break;
      }
    }
    
    Iterable<DataNode> nodes = dataNode.getNodes();
    for (Iterator<DataNode> it = nodes.iterator();it.hasNext();) {
      DataNode subNode = it.next();
      IMetaStoreAttribute subAttr = newAttribute(subNode.getName(), null);
      dataNodeToAttribute(subNode, subAttr);
      attribute.addChild(subAttr);
    }
  }
  
  protected RepositoryFile validateNamespace(String namespace) throws MetaStoreException {
    RepositoryFile namespaceFile = getNamespaceRepositoryFile(namespace);
    if (namespaceFile==null) {
      throw new MetaStoreException("Namespace '"+namespace+" doesn't exist in the repository");
    }
    return namespaceFile;
  }
  
  protected RepositoryFile validateElementTypeRepositoryFolder(String elementTypeId) throws MetaStoreException {
    RepositoryFile elementTypeFolder = getElementTypeRepositoryFolder(elementTypeId);
    if (elementTypeFolder==null) {
      throw new MetaStoreException("The element type with id '"+elementTypeId+" doesn't exist");
    }
    return elementTypeFolder;
  }
    
  protected RepositoryFile getNamespaceRepositoryFile(String namespace) {
    return findChild(namespacesFolder.getId(), namespace);
  }

  protected RepositoryFile getElementTypeRepositoryFolder(String elementTypeId) {
    return pur.getFileById(elementTypeId);    
  }

  protected RepositoryFile getElementTypeRepositoryFileByName(String namespace, String elementTypeName) {
    RepositoryFile namespaceFolder = getNamespaceRepositoryFile(namespace);
    if (namespace==null) {
      return null;
    }
    return findChild(namespaceFolder.getId(), elementTypeName);
  }

  protected RepositoryFile buildFolder(String path, String foldername) {
    return buildFolder(path, foldername, null, null);  
  }

  protected RepositoryFile buildFolder(String path, String foldername, String title, String description) {
    return new RepositoryFile(null, foldername, true, false, false, null, path, new Date(), new Date(), false, null, null, null, null, title, description, null, null, 0L, null, null);  
  }

  protected String getMetaStoreParentFolderPath() {
    return ClientRepositoryPaths.getEtcFolderPath() + RepositoryFile.SEPARATOR + FOLDER_METASTORE;
  }

  protected RepositoryFile findChild(Serializable folderId, String childName) {
    for (RepositoryFile child : pur.getChildren(folderId)) {
      if (child.getName().equals(childName)) {
        return child;
      }
    }
    return null;
  }
  
  protected RepositoryFileAcl getAcls() {
    return null; // new RepositoryFileAcl.Builder(RepositoryFileAcl.Builder).entriesInheriting(true).build()
  }
}
