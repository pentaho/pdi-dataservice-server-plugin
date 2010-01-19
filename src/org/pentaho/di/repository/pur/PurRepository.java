package org.pentaho.di.repository.pur;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.cluster.ClusterSchema;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ProgressMonitorListener;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleSecurityException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.partition.PartitionSchema;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.ObjectRevision;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.repository.RepositoryElementLocationInterface;
import org.pentaho.di.repository.RepositoryLock;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.RepositoryObject;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.repository.RepositorySecurityProvider;
import org.pentaho.di.repository.RepositoryVersionRegistry;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.platform.api.engine.IPentahoSession;
import org.pentaho.platform.api.repository.IRepositoryService;
import org.pentaho.platform.api.repository.RepositoryFile;
import org.pentaho.platform.api.repository.VersionSummary;
import org.pentaho.platform.engine.core.system.PentahoSessionHolder;
import org.pentaho.platform.engine.core.system.StandaloneSession;
import org.pentaho.platform.repository.pcr.RepositoryPaths;
import org.pentaho.platform.repository.pcr.data.node.NodeRepositoryFileData;
import org.springframework.security.Authentication;
import org.springframework.security.GrantedAuthority;
import org.springframework.security.GrantedAuthorityImpl;
import org.springframework.security.context.SecurityContextHolder;
import org.springframework.security.providers.UsernamePasswordAuthenticationToken;
import org.springframework.security.userdetails.User;
import org.springframework.security.userdetails.UserDetails;

import com.pentaho.commons.dsc.PentahoDscContent;
import com.pentaho.commons.dsc.PentahoLicenseVerifier;
import com.pentaho.commons.dsc.params.KParam;

/**
 * Implementation of {@link Repository} that delegates to an {@link IRepositoryService} instance.
 * 
 * @author Matt
 * @author mlowery
 */
public class PurRepository implements Repository {

  // ~ Static fields/initializers ======================================================================================

  private static final Log logger = LogFactory.getLog(PurRepository.class);

  private static final String REPOSITORY_VERSION = "1.0"; //$NON-NLS-1$

  private static final boolean VERSION_SHARED_OBJECTS = true;

  // ~ Instance fields =================================================================================================

  private IRepositoryService pur;

  private UserInfo userInfo;

  private RepositoryMeta repositoryMeta;

  private ITransformer databaseMetaTransformer = new DatabaseDelegate(this);

  private ITransformer partitionSchemaTransformer = new PartitionDelegate(this);
  
  private ITransformer slaveTransformer = new SlaveDelegate(this);

  private ISharedObjectsTransformer transDelegate = new TransDelegate(this);

  private ISharedObjectsTransformer jobDelegate = new JobDelegate(this);

  // ~ Constructors ====================================================================================================

  public PurRepository() {
    super();
  }

  // ~ Methods =========================================================================================================

  public void setPur(final IRepositoryService pur) {
    this.pur = pur;
  }

  public void init(final RepositoryMeta repositoryMeta, final UserInfo userInfo) {
    this.repositoryMeta = repositoryMeta;
    this.userInfo = userInfo;
  }

  public void connect() throws KettleException, KettleSecurityException {
    setupUser();
  }

  /**
   * Creates {@link IPentahoSession} and sets it using {@link PentahoSessionHolder}. Creates {@link Authentication} and 
   * sets it using {@link SecurityContextHolder}. Finally, kicks off PUR events.
   */
  private void setupUser() {
    StandaloneSession pentahoSession = new StandaloneSession(userInfo.getLogin());
    pentahoSession.setAuthenticated(userInfo.getLogin());
    pentahoSession.setAttribute(IPentahoSession.TENANT_ID_KEY, "acme");
    final GrantedAuthority[] authorities = new GrantedAuthority[2];
    authorities[0] = new GrantedAuthorityImpl("Authenticated");
    authorities[1] = new GrantedAuthorityImpl("acme_Authenticated");
    final String password = "ignored"; //$NON-NLS-1$
    UserDetails userDetails = new User(userInfo.getLogin(), password, true, true, true, true, authorities);
    Authentication authentication = new UsernamePasswordAuthenticationToken(userDetails, password, authorities);
    // next line is copy of SecurityHelper.setPrincipal
    pentahoSession.setAttribute("SECURITY_PRINCIPAL", authentication);
    PentahoSessionHolder.setSession(pentahoSession);
    SecurityContextHolder.getContext().setAuthentication(authentication);
    pur.getRepositoryEventHandler().onNewTenant();
    pur.getRepositoryEventHandler().onNewUser();
  }

  public boolean isConnected() {
    return PentahoSessionHolder.getSession() != null;
  }

  public void disconnect() {
    PentahoSessionHolder.removeSession();
  }

  public int countNrJobEntryAttributes(ObjectId idJobentry, String code) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public int countNrStepAttributes(ObjectId idStep, String code) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public RepositoryDirectory createRepositoryDirectory(final RepositoryDirectory parentDirectory,
      final String directoryPath) throws KettleException {
    try {
      String[] path = Const.splitPath(directoryPath, RepositoryDirectory.DIRECTORY_SEPARATOR);

      RepositoryDirectory follow = parentDirectory;
      for (int level = 0; level < path.length; level++) {
        RepositoryDirectory child = follow.findChild(path[level]);
        if (child == null) {
          // create this one
          //
          child = new RepositoryDirectory(follow, path[level]);
          PentahoDscContent dscContent = PentahoLicenseVerifier.verify(new KParam());
          if (dscContent.getHolder() != null) {
            saveRepositoryDirectory(child);
          }
        }

        follow = child;
      }
      return follow;
    } catch (Exception e) {
      throw new KettleException("Unable to create directory with path [" + directoryPath + "]", e);
    }
  }

  public void saveRepositoryDirectory(final RepositoryDirectory dir) throws KettleException {
    try {
      PentahoDscContent dscContent = PentahoLicenseVerifier.verify(new KParam());
      if (dscContent.getSubject() != null) {
        // id of root dir is null--check for it
        RepositoryFile newFolder = pur.createFolder(dir.getParent().getObjectId() != null ? dir.getParent()
            .getObjectId().getId() : null, new RepositoryFile.Builder(dir.getName()).folder(true).build());
        dir.setObjectId(new StringObjectId(newFolder.getId().toString()));
      }
    } catch (Exception e) {
      throw new KettleException("Unable to save repository directory with path [" + dir.getPath() + "]", e);
    }
  }

  public void deleteRepositoryDirectory(final RepositoryDirectory dir) throws KettleException {
    try {
      pur.permanentlyDeleteFile(dir.getObjectId().getId());
    } catch (Exception e) {
      throw new KettleException("Unable to delete directory with path [" + dir.getPath() + "]", e);
    }
  }

  public ObjectId renameRepositoryDirectory(final RepositoryDirectory dir) throws KettleException {
    // dir ID is used to find orig obj; dir name is new name of obj; dir is not moved from its original loc
    try {
      pur.moveFile(dir.getObjectId().getId(), dir.getParent().getPath() + RepositoryFile.SEPARATOR + dir.getName());
      return dir.getObjectId();
    } catch (Exception e) {
      throw new KettleException("Unable to rename directory with id [" + dir.getObjectId() + "] to [" + dir.getName()
          + "]", e);
    }
  }

  public ObjectId renameRepositoryDirectory(final ObjectId dirId, final RepositoryDirectory newParent,
      final String newName) throws KettleException {
    // dir ID is used to find orig obj; new parent is used as new parent (might be null meaning no change in parent); 
    // new name is used as new file name (might be null meaning no change in name)
    String finalName = null;
    String finalParentPath = null;
    try {
      RepositoryFile folder = pur.getFileById(dirId.getId());
      finalName = (newName != null ? newName : folder.getName());
      finalParentPath = (newParent != null ? newParent.getPath() : folder.getAbsolutePath());
      pur.moveFile(dirId.getId(), finalParentPath + RepositoryFile.SEPARATOR + finalName);
      return dirId;
    } catch (Exception e) {
      throw new KettleException("Unable to move/rename directory with id [" + dirId + "] to new parent ["
          + finalParentPath + "] and new name [" + finalName + "]", e);
    }
  }

  public RepositoryDirectory loadRepositoryDirectoryTree() throws KettleException {
    RepositoryDirectory rootDir = new RepositoryDirectory();
    RepositoryFile pentahoRootFolder = pur.getFile(RepositoryPaths.getPentahoRootFolderPath());
    RepositoryDirectory dir = new RepositoryDirectory(rootDir, pentahoRootFolder.getName());
    dir.setObjectId(new StringObjectId(pentahoRootFolder.getId().toString()));
    rootDir.addSubdirectory(dir);
    loadRepositoryDirectory(dir, pentahoRootFolder);
    rootDir.setObjectId(null);
    return rootDir;
  }

  private void loadRepositoryDirectory(final RepositoryDirectory parentDir, final RepositoryFile folder)
      throws KettleException {
    try {
      List<RepositoryFile> children = pur.getChildren(folder.getId());
      for (RepositoryFile child : children) {
        RepositoryDirectory dir = new RepositoryDirectory(parentDir, child.getName());
        dir.setObjectId(new StringObjectId(child.getId().toString()));
        parentDir.addSubdirectory(dir);
        if (child.isFolder()) {
          loadRepositoryDirectory(dir, child);
        }
      }
    } catch (Exception e) {
      throw new KettleException("Unable to load directory structure from repository", e);
    }
  }

  public String[] getDirectoryNames(final ObjectId idDirectory) throws KettleException {
    try {
      List<RepositoryFile> children = pur.getChildren(idDirectory.getId());
      List<String> childNames = new ArrayList<String>();
      for (RepositoryFile child : children) {
        childNames.add(child.getName());
      }
      return childNames.toArray(new String[0]);
    } catch (Exception e) {
      throw new KettleException("Unable to get list of object names from directory [" + idDirectory + "]", e);
    }
  }

  public void deleteClusterSchema(ObjectId idCluster) throws KettleException {

    // TODO Auto-generated method stub 

  }

  public void deleteJob(ObjectId idJob) throws KettleException {
    deleteFileById(idJob);
  }

  public void deleteFileById(final ObjectId id) throws KettleException {
    try {
      RepositoryFile fileToDelete = pur.getFileById(id.getId());
      pur.deleteFile(fileToDelete.getId());
    } catch (Exception e) {
      throw new KettleException("Unable to delete object with id [" + id + "]", e);
    }
  }

  public void deletePartitionSchema(ObjectId idPartitionSchema) throws KettleException {
    try {
      RepositoryFile fileToDelete = pur.getFileById(idPartitionSchema.getId());
      pur.deleteFile(fileToDelete.getId());
    } catch (Exception e) {
      throw new KettleException("Unable to delete partition schema with name [" + idPartitionSchema + "]", e);
    }
  }

  public void deleteSlave(ObjectId idSlave) throws KettleException {
    try {
      RepositoryFile fileToDelete = pur.getFileById(idSlave.getId());
      pur.deleteFile(fileToDelete.getId());
    } catch (Exception e) {
      throw new KettleException("Unable to delete slave with name [" + idSlave + "]", e);
    }
  }

  public void deleteTransformation(ObjectId idTransformation) throws KettleException {
    deleteFileById(idTransformation);
  }

  public boolean exists(final String name, final RepositoryDirectory repositoryDirectory,
      final RepositoryObjectType objectType) throws KettleException {
    try {
      String absPath = getPath(name, repositoryDirectory, objectType);
      return pur.getFile(absPath) != null;
    } catch (Exception e) {
      throw new KettleException("Unable to verify if the repository element [" + name + "] exists in ", e);
    }
  }

  private String getPath(final String name, final RepositoryDirectory repositoryDirectory,
      final RepositoryObjectType objectType) {
    switch (objectType) {
      case DATABASE: {
        return getDatabaseMetaParentFolderPath() + RepositoryFile.SEPARATOR + name
            + RepositoryObjectType.DATABASE.getExtension();
      }
      case TRANSFORMATION: {
        return repositoryDirectory.getPath() + RepositoryFile.SEPARATOR + name
            + RepositoryObjectType.TRANSFORMATION.getExtension();
      }
      case PARTITION_SCHEMA: {
        return getPartitionSchemaParentFolderPath() + RepositoryFile.SEPARATOR + name
            + RepositoryObjectType.PARTITION_SCHEMA.getExtension();
      }
      case SLAVE_SERVER: {
        return getSlaveParentFolderPath() + RepositoryFile.SEPARATOR + name
            + RepositoryObjectType.SLAVE_SERVER.getExtension();
      }      

      case JOB: {
        return repositoryDirectory.getPath() + RepositoryFile.SEPARATOR + name
            + RepositoryObjectType.JOB.getExtension();
      }
      default: {
        throw new UnsupportedOperationException("not implemented");
      }
    }
  }

  public ObjectId getClusterID(String name) throws KettleException {

    // TODO Auto-generated method stub 
    return null;

  }

  public ObjectId[] getClusterIDs(boolean includeDeleted) throws KettleException {

    // TODO Auto-generated method stub 
    return new ObjectId[0];

  }

  public String[] getClusterNames(boolean includeDeleted) throws KettleException {

    // TODO Auto-generated method stub 
    return null;

  }

  public ObjectId getDatabaseID(final String name) throws KettleException {
    try {
      return getObjectId(name, null, RepositoryObjectType.DATABASE);
    } catch (Exception e) {
      throw new KettleException("Unable to get ID for database [" + name + "]", e);
    }
  }

  /**
   * Copying the behavior of the original JCRRepository, this implementation returns IDs of deleted objects too.
   */
  private ObjectId getObjectId(final String name, final RepositoryDirectory dir, final RepositoryObjectType objectType) {
    final String absPath = getPath(name, dir, objectType);
    RepositoryFile file = pur.getFile(absPath);
    if (file != null) {
      // file exists
      return new StringObjectId(file.getId().toString());
    } else {
      switch (objectType) {
        case DATABASE: {
          // file either never existed or has been deleted
          RepositoryFile databaseMetaParentFolder = pur.getFile(getDatabaseMetaParentFolderPath());
          List<RepositoryFile> deletedChildren = pur.getDeletedFiles(databaseMetaParentFolder.getId(), name
              + RepositoryObjectType.DATABASE.getExtension());
          if (!deletedChildren.isEmpty()) {
            return new StringObjectId(deletedChildren.get(0).getId().toString());
          } else {
            return null;
          }
        }
        case TRANSFORMATION: {
          // file either never existed or has been deleted
          RepositoryFile transParentFolder = pur.getFile(dir.getPath());
          List<RepositoryFile> deletedChildren = pur.getDeletedFiles(transParentFolder.getId(), name
              + RepositoryObjectType.TRANSFORMATION.getExtension());
          if (!deletedChildren.isEmpty()) {
            return new StringObjectId(deletedChildren.get(0).getId().toString());
          } else {
            return null;
          }
        }
        case PARTITION_SCHEMA: {
          // file either never existed or has been deleted
          RepositoryFile partitionParentFolder = pur.getFile(getPartitionSchemaParentFolderPath());
          List<RepositoryFile> deletedChildren = pur.getDeletedFiles(partitionParentFolder.getId(), name
              + RepositoryObjectType.PARTITION_SCHEMA.getExtension());
          if (!deletedChildren.isEmpty()) {
            return new StringObjectId(deletedChildren.get(0).getId().toString());
          } else {
            return null;
          }
        }
        case SLAVE_SERVER: {
          // file either never existed or has been deleted
          RepositoryFile slaveParentFolder = pur.getFile(getSlaveParentFolderPath());
          List<RepositoryFile> deletedChildren = pur.getDeletedFiles(slaveParentFolder.getId(), name
              + RepositoryObjectType.SLAVE_SERVER.getExtension());
          if (!deletedChildren.isEmpty()) {
            return new StringObjectId(deletedChildren.get(0).getId().toString());
          } else {
            return null;
          }
        }        
        case JOB: {
          // file either never existed or has been deleted
          RepositoryFile jobParentFolder = pur.getFile(dir.getPath());
          List<RepositoryFile> deletedChildren = pur.getDeletedFiles(jobParentFolder.getId(), name
              + RepositoryObjectType.JOB.getExtension());
          if (!deletedChildren.isEmpty()) {
            return new StringObjectId(deletedChildren.get(0).getId().toString());
          } else {
            return null;
          }
        }
        default: {
          throw new UnsupportedOperationException("not implemented");
        }
      }
    }
  }

  public ObjectId[] getDatabaseIDs(boolean includeDeleted) throws KettleException {
    try {
      List<RepositoryFile> children = getAllFilesOfType(null, RepositoryObjectType.DATABASE, includeDeleted);
      List<ObjectId> ids = new ArrayList<ObjectId>();
      for (RepositoryFile file : children) {
        ids.add(new StringObjectId(file.getId().toString()));
      }
      return ids.toArray(new ObjectId[0]);
    } catch (Exception e) {
      throw new KettleException("Unable to get all database IDs", e);
    }
  }

  protected List<RepositoryFile> getAllFilesOfType(final RepositoryDirectory dir,
      final RepositoryObjectType objectType, final boolean includeDeleted) throws KettleException {

    List<RepositoryFile> allChildren = new ArrayList<RepositoryFile>();
    List<RepositoryFile> children = getAllFilesOfType(dir, objectType);
    allChildren.addAll(children);
    if (includeDeleted) {
      List<RepositoryFile> deletedChildren = getAllDeletedFilesOfType(dir, objectType);
      allChildren.addAll(deletedChildren);
      Collections.sort(allChildren);
    }
    return allChildren;
  }

  protected List<RepositoryFile> getAllFilesOfType(final RepositoryDirectory dir, final RepositoryObjectType objectType)
      throws KettleException {
    RepositoryFile parentFolder = null;
    String filter = null;
    switch (objectType) {
      case DATABASE: {
        parentFolder = pur.getFile(getDatabaseMetaParentFolderPath());
        filter = "*" + RepositoryObjectType.DATABASE.getExtension(); //$NON-NLS-1$
        break;
      }
      case TRANSFORMATION: {
        parentFolder = pur.getFile(dir.getPath());
        filter = "*" + RepositoryObjectType.TRANSFORMATION.getExtension(); //$NON-NLS-1$
        break;
      }
      case PARTITION_SCHEMA: {
        parentFolder = pur.getFile(getPartitionSchemaParentFolderPath());
        filter = "*" + RepositoryObjectType.PARTITION_SCHEMA.getExtension(); //$NON-NLS-1$
        break;
      }
      case SLAVE_SERVER: {
        parentFolder = pur.getFile(getSlaveParentFolderPath());
        filter = "*" + RepositoryObjectType.SLAVE_SERVER.getExtension(); //$NON-NLS-1$
        break;
      }      
      case JOB: {
        parentFolder = pur.getFile(dir.getPath());
        filter = "*" + RepositoryObjectType.JOB.getExtension(); //$NON-NLS-1$
        break;
      }
      default: {
        throw new UnsupportedOperationException("not implemented");
      }
    }
    return pur.getChildren(parentFolder.getId(), filter);
  }

  protected List<RepositoryFile> getAllDeletedFilesOfType(final RepositoryDirectory dir,
      final RepositoryObjectType objectType) throws KettleException {
    RepositoryFile parentFolder = null;
    String filter = null;
    switch (objectType) {
      case DATABASE: {
        parentFolder = pur.getFile(getDatabaseMetaParentFolderPath());
        filter = "*" + RepositoryObjectType.DATABASE.getExtension(); //$NON-NLS-1$
        break;
      }
      case TRANSFORMATION: {
        parentFolder = pur.getFile(dir.getPath());
        filter = "*" + RepositoryObjectType.TRANSFORMATION.getExtension(); //$NON-NLS-1$
        break;
      }
      case PARTITION_SCHEMA: {
        parentFolder = pur.getFile(getPartitionSchemaParentFolderPath());
        filter = "*" + RepositoryObjectType.PARTITION_SCHEMA.getExtension(); //$NON-NLS-1$
        break;
      }
      case SLAVE_SERVER: {
        parentFolder = pur.getFile(getSlaveParentFolderPath());
        filter = "*" + RepositoryObjectType.SLAVE_SERVER.getExtension(); //$NON-NLS-1$
        break;
      }      
      case JOB: {
        parentFolder = pur.getFile(dir.getPath());
        filter = "*" + RepositoryObjectType.JOB.getExtension(); //$NON-NLS-1$
        break;
      }
      default: {
        throw new UnsupportedOperationException("not implemented");
      }
    }
    return pur.getDeletedFiles(parentFolder.getId(), filter);
  }

  public String[] getDatabaseNames(boolean includeDeleted) throws KettleException {
    try {
      List<RepositoryFile> children = getAllFilesOfType(null, RepositoryObjectType.DATABASE, includeDeleted);
      List<String> names = new ArrayList<String>();
      for (RepositoryFile file : children) {
        // strip off extension
        names.add(file.getName().substring(0,
            file.getName().length() - RepositoryObjectType.DATABASE.getExtension().length()));
      }
      return names.toArray(new String[0]);
    } catch (Exception e) {
      throw new KettleException("Unable to get all database names", e);
    }
  }

  public List<DatabaseMeta> readDatabases() throws KettleException {
    try {
      List<RepositoryFile> children = getAllFilesOfType(null, RepositoryObjectType.DATABASE, false);
      List<DatabaseMeta> dbMetas = new ArrayList<DatabaseMeta>();
      for (RepositoryFile file : children) {
        dbMetas.add((DatabaseMeta) databaseMetaTransformer.dataNodeToElement(pur.getDataForRead(file.getId(),
            NodeRepositoryFileData.class).getNode()));
      }
      return dbMetas;
    } catch (Exception e) {
      throw new KettleException("Unable to read all databases", e);
    }
  }

  public void deleteDatabaseMeta(final String databaseName) throws KettleException {
    try {
      RepositoryFile fileToDelete = pur.getFile(getPath(databaseName, null, RepositoryObjectType.DATABASE));
      pur.deleteFile(fileToDelete.getId());
    } catch (Exception e) {
      throw new KettleException("Unable to delete database with name [" + databaseName + "]", e);
    }
  }

  public boolean getJobEntryAttributeBoolean(ObjectId idJobentry, String code) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public boolean getJobEntryAttributeBoolean(ObjectId idJobentry, int nr, String code) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public boolean getJobEntryAttributeBoolean(ObjectId idJobentry, String code, boolean def) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public long getJobEntryAttributeInteger(ObjectId idJobentry, String code) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public long getJobEntryAttributeInteger(ObjectId idJobentry, int nr, String code) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public String getJobEntryAttributeString(ObjectId idJobentry, String code) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public String getJobEntryAttributeString(ObjectId idJobentry, int nr, String code) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public ObjectId getJobId(final String name, final RepositoryDirectory repositoryDirectory) throws KettleException {
    try {
      return getObjectId(name, repositoryDirectory, RepositoryObjectType.JOB);
    } catch (Exception e) {
      throw new KettleException("Unable to get ID for job [" + name + "]", e);
    }
  }

  public RepositoryLock getJobLock(ObjectId idJob) throws KettleException {
    return getLockById(idJob);
  }

  public String[] getJobNames(ObjectId idDirectory, boolean includeDeleted) throws KettleException {
    try {
      RepositoryDirectory dir = loadRepositoryDirectoryTree().findDirectory(idDirectory);
      List<RepositoryFile> children = getAllFilesOfType(dir, RepositoryObjectType.JOB, includeDeleted);
      List<String> names = new ArrayList<String>();
      for (RepositoryFile file : children) {
        // strip off extension
        names.add(file.getName().substring(0,
            file.getName().length() - RepositoryObjectType.JOB.getExtension().length()));
      }
      return names.toArray(new String[0]);
    } catch (Exception e) {
      throw new KettleException("Unable to get all job names", e);
    }
  }

  public List<RepositoryObject> getJobObjects(ObjectId idDirectory, boolean includeDeleted) throws KettleException {
    return getPdiObjects(idDirectory, RepositoryObjectType.JOB, includeDeleted);
  }

  public LogChannelInterface getLog() {

    // TODO Auto-generated method stub 
    return null;

  }

  public String getName() {
    return repositoryMeta.getName();
  }

  public ObjectId getPartitionSchemaID(String name) throws KettleException {
    try {
      return getObjectId(name, null, RepositoryObjectType.PARTITION_SCHEMA);
    } catch (Exception e) {
      throw new KettleException("Unable to get ID for partition schema [" + name + "]", e);
    }
  }

  public ObjectId[] getPartitionSchemaIDs(boolean includeDeleted) throws KettleException {
    try {
      List<RepositoryFile> children = getAllFilesOfType(null, RepositoryObjectType.PARTITION_SCHEMA, includeDeleted);
      List<ObjectId> ids = new ArrayList<ObjectId>();
      for (RepositoryFile file : children) {
        ids.add(new StringObjectId(file.getId().toString()));
      }
      return ids.toArray(new ObjectId[0]);
    } catch (Exception e) {
      throw new KettleException("Unable to get all partition schema IDs", e);
    }
  }

  public String[] getPartitionSchemaNames(boolean includeDeleted) throws KettleException {
    try {
      List<RepositoryFile> children = getAllFilesOfType(null, RepositoryObjectType.PARTITION_SCHEMA, includeDeleted);
      List<String> names = new ArrayList<String>();
      for (RepositoryFile file : children) {
        // strip off extension
        names.add(file.getName().substring(0,
            file.getName().length() - RepositoryObjectType.PARTITION_SCHEMA.getExtension().length()));
      }
      return names.toArray(new String[0]);
    } catch (Exception e) {
      throw new KettleException("Unable to get all partition schema names", e);
    }
  }

  public RepositoryMeta getRepositoryMeta() {
    return repositoryMeta;
  }

  /**
   * The implementation of this method is more complex because it takes a {@link RepositoryElementLocationInterface}
   * which does not have an ID.
   */
  public List<ObjectRevision> getRevisions(final RepositoryElementLocationInterface element) throws KettleException {
    String absPath = null;
    try {
      List<ObjectRevision> versions = new ArrayList<ObjectRevision>();
      absPath = getPath(element.getName(), element.getRepositoryDirectory(), element.getRepositoryElementType());
      RepositoryFile file = pur.getFile(absPath);
      List<VersionSummary> versionSummaries = pur.getVersionSummaries(file.getId());
      for (VersionSummary versionSummary : versionSummaries) {
        versions.add(new PurObjectRevision(versionSummary.getId(), versionSummary.getAuthor(),
            versionSummary.getDate(), versionSummary.getMessage()));
      }
      return versions;
    } catch (Exception e) {
      throw new KettleException("Could not retrieve version history of object with path [" + absPath + "]", e);
    }
  }

  public RepositorySecurityProvider getSecurityProvider() {

    // TODO Auto-generated method stub 
    return null;

  }

  public ObjectId getSlaveID(String name) throws KettleException {
    try {
      return getObjectId(name, null, RepositoryObjectType.SLAVE_SERVER);
    } catch (Exception e) {
      throw new KettleException("Unable to get ID for slave server with name [" + name + "]", e);
    }
  }

  public ObjectId[] getSlaveIDs(boolean includeDeleted) throws KettleException {
    try {
      List<RepositoryFile> children = getAllFilesOfType(null, RepositoryObjectType.SLAVE_SERVER, includeDeleted);
      List<ObjectId> ids = new ArrayList<ObjectId>();
      for (RepositoryFile file : children) {
        ids.add(new StringObjectId(file.getId().toString()));
      }
      return ids.toArray(new ObjectId[0]);
    } catch (Exception e) {
      throw new KettleException("Unable to get all slave server IDs", e);
    }
  }

  public String[] getSlaveNames(boolean includeDeleted) throws KettleException {
    try {
      List<RepositoryFile> children = getAllFilesOfType(null, RepositoryObjectType.SLAVE_SERVER, includeDeleted);
      List<String> names = new ArrayList<String>();
      for (RepositoryFile file : children) {
        // strip off extension
        names.add(file.getName().substring(0,
            file.getName().length() - RepositoryObjectType.SLAVE_SERVER.getExtension().length()));
      }
      return names.toArray(new String[0]);
    } catch (Exception e) {
      throw new KettleException("Unable to get all slave server names", e);
    }
  }

  public List<SlaveServer> getSlaveServers() throws KettleException {
    try {
      List<SlaveServer> list = new ArrayList<SlaveServer>();
      
      ObjectId[] ids = getSlaveIDs(false);
      for (ObjectId id : ids) {
        list.add( loadSlaveServer(id, null) ); // the last version
      }
      
      return list;
    } catch(Exception e) {
      throw new KettleException("Unable to load all slave servers from the repository", e);
    }  }

  public boolean getStepAttributeBoolean(ObjectId idStep, String code) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public boolean getStepAttributeBoolean(ObjectId idStep, int nr, String code) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public boolean getStepAttributeBoolean(ObjectId idStep, int nr, String code, boolean def) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public long getStepAttributeInteger(ObjectId idStep, String code) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public long getStepAttributeInteger(ObjectId idStep, int nr, String code) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public String getStepAttributeString(ObjectId idStep, String code) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public String getStepAttributeString(ObjectId idStep, int nr, String code) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public ObjectId getTransformationID(String name, RepositoryDirectory repositoryDirectory) throws KettleException {
    try {
      return getObjectId(name, repositoryDirectory, RepositoryObjectType.TRANSFORMATION);
    } catch (Exception e) {
      throw new KettleException("Unable to get ID for transformation [" + name + "]", e);
    }
  }

  public RepositoryLock getTransformationLock(ObjectId idTransformation) throws KettleException {
    return getLockById(idTransformation);
  }

  protected RepositoryLock getLockById(final ObjectId id) throws KettleException {
    RepositoryFile file = pur.getFileById(id.getId());
    return getLock(file);
  }

  public String[] getTransformationNames(ObjectId idDirectory, boolean includeDeleted) throws KettleException {
    try {
      RepositoryDirectory dir = loadRepositoryDirectoryTree().findDirectory(idDirectory);
      List<RepositoryFile> children = getAllFilesOfType(dir, RepositoryObjectType.TRANSFORMATION, includeDeleted);
      List<String> names = new ArrayList<String>();
      for (RepositoryFile file : children) {
        // strip off extension
        names.add(file.getName().substring(0,
            file.getName().length() - RepositoryObjectType.TRANSFORMATION.getExtension().length()));
      }
      return names.toArray(new String[0]);
    } catch (Exception e) {
      throw new KettleException("Unable to get all transformation names", e);
    }
  }

  public List<RepositoryObject> getTransformationObjects(ObjectId idDirectory, boolean includeDeleted)
      throws KettleException {
    return getPdiObjects(idDirectory, RepositoryObjectType.TRANSFORMATION, includeDeleted);
  }

  protected List<RepositoryObject> getPdiObjects(ObjectId id_directory, RepositoryObjectType objectType,
      boolean includeDeleted) throws KettleException {
    try {
      RepositoryDirectory repDir = loadRepositoryDirectoryTree().findDirectory(id_directory);

      List<RepositoryObject> list = new ArrayList<RepositoryObject>();
      List<RepositoryFile> nonDeletedChildren = getAllFilesOfType(repDir, objectType);
      for (RepositoryFile file : nonDeletedChildren) {
        VersionSummary v = pur.getVersionSummary(file.getId(), file.getVersionId());
        String description = file.getDescription() + " - v" + v.getId();
        RepositoryLock lock = getLock(file);
        String lockMessage = lock == null ? null : lock.getMessage() + " (" + lock.getLogin() + " since "
            + XMLHandler.date2string(lock.getLockDate()) + ")";
        list.add(new RepositoryObject(new StringObjectId(file.getId().toString()), file.getName().substring(0,
            file.getName().length() - objectType.getExtension().length()), repDir, v.getAuthor(), v.getDate(),
            objectType, description, lockMessage, false));
      }
      if (includeDeleted) {
        List<RepositoryFile> deletedChildren = getAllDeletedFilesOfType(repDir, objectType);
        for (RepositoryFile file : deletedChildren) {
          VersionSummary v = pur.getVersionSummary(file.getId(), file.getVersionId());
          String description = file.getDescription() + " - v" + v.getId();
          RepositoryLock lock = getLock(file);
          String lockMessage = lock == null ? null : lock.getMessage() + " (" + lock.getLogin() + " since "
              + XMLHandler.date2string(lock.getLockDate()) + ")";
          list.add(new RepositoryObject(new StringObjectId(file.getId().toString()), file.getName().substring(0,
              file.getName().length() - objectType.getExtension().length()), repDir, v.getAuthor(), v.getDate(),
              objectType, description, lockMessage, true));
        }
      }
      return list;
    } catch (Exception e) {
      throw new KettleException("Unable to get list of objects from directory [" + id_directory + "]", e);
    }
  }

  public UserInfo getUserInfo() {
    return userInfo;
  }

  public String getVersion() {
    return REPOSITORY_VERSION;
  }

  public RepositoryVersionRegistry getVersionRegistry() throws KettleException {

    // TODO Auto-generated method stub 
    return null;

  }

  public void insertJobEntryDatabase(ObjectId idJob, ObjectId idJobentry, ObjectId idDatabase) throws KettleException {

    // TODO Auto-generated method stub 

  }

  public ObjectId insertLogEntry(String description) throws KettleException {

    // TODO Auto-generated method stub 
    return null;

  }

  public void insertStepDatabase(ObjectId idTransformation, ObjectId idStep, ObjectId idDatabase)
      throws KettleException {

    // TODO Auto-generated method stub 

  }

  public ClusterSchema loadClusterSchema(ObjectId idClusterSchema, List<SlaveServer> slaveServers, String versionLabel)
      throws KettleException {

    // TODO Auto-generated method stub 
    return null;

  }

  public Condition loadConditionFromStepAttribute(ObjectId idStep, String code) throws KettleException {

    // TODO Auto-generated method stub 
    return null;

  }

  public DatabaseMeta loadDatabaseMetaFromJobEntryAttribute(ObjectId idJobentry, String nameCode, String idCode,
      List<DatabaseMeta> databases) throws KettleException {

    // TODO Auto-generated method stub 
    return null;

  }

  public DatabaseMeta loadDatabaseMetaFromStepAttribute(ObjectId idStep, String code, List<DatabaseMeta> databases)
      throws KettleException {

    // TODO Auto-generated method stub 
    return null;

  }

  public PartitionSchema loadPartitionSchema(ObjectId partitionSchemaId, String versionId) throws KettleException {
    try {
      PartitionSchema partitionSchema = new PartitionSchema();
      partitionSchemaTransformer.dataNodeToElement(pur.getDataForRead(partitionSchemaId.getId(), versionId,
          NodeRepositoryFileData.class).getNode(), partitionSchema);
      partitionSchema.setObjectId(new StringObjectId(partitionSchemaId));
      partitionSchema.setObjectRevision(getObjectRevision(partitionSchemaId, versionId));
      partitionSchema.clearChanged();
      return partitionSchema;
    } catch (Exception e) {
      throw new KettleException("Unable to load partition schema with id [" + partitionSchemaId + "]", e);
    }
  }

  public SlaveServer loadSlaveServer(ObjectId idSlaveServer, String versionId) throws KettleException {
    try {
      SlaveServer slaveServer = new SlaveServer();
      slaveTransformer.dataNodeToElement(pur.getDataForRead(idSlaveServer.getId(), versionId,
          NodeRepositoryFileData.class).getNode(), slaveServer);
      slaveServer.setObjectId(new StringObjectId(idSlaveServer));
      slaveServer.setObjectRevision(getObjectRevision(idSlaveServer, versionId));
      slaveServer.clearChanged();
      return slaveServer;
    } catch (Exception e) {
      throw new KettleException("Unable to load slave server with id [" + idSlaveServer + "]", e);
    }
  }

  public void lockJob(final ObjectId idJob, final String message) throws KettleException {
    lockFileById(idJob, message);
  }

  public void lockTransformation(final ObjectId idTransformation, final String message) throws KettleException {
    lockFileById(idTransformation, message);
  }

  protected void lockFileById(final ObjectId id, final String message) throws KettleException {
    pur.lockFile(id.getId(), message);
  }

  public SharedObjects readJobMetaSharedObjects(JobMeta jobMeta) throws KettleException {

    // TODO Auto-generated method stub 
    return null;

  }

  public SharedObjects readTransSharedObjects(TransMeta transMeta) throws KettleException {

    // TODO Auto-generated method stub 
    return null;

  }

  public ObjectId renameDatabase(ObjectId idDatabase, String newname) throws KettleException {

    // TODO Auto-generated method stub 
    return null;

  }

  public ObjectId renameJob(ObjectId idJob, RepositoryDirectory newDirectory, String newName) throws KettleException {

    // TODO Auto-generated method stub 
    return null;

  }

  public ObjectId renameTransformation(ObjectId idTransformation, RepositoryDirectory newDirectory, String newName)
      throws KettleException {

    // TODO Auto-generated method stub 
    return null;

  }

  public void save(final RepositoryElementInterface element, final String versionComment,
      final ProgressMonitorListener monitor) throws KettleException {
    PentahoDscContent dscContent = PentahoLicenseVerifier.verify(new KParam());
    if (dscContent.getSubject() == null) {
      return;
    }

    try {
      switch (element.getRepositoryElementType()) {
        case TRANSFORMATION:
          saveTrans(element, versionComment);
          break;
        case JOB:
          saveJob(element, versionComment);
          break;
        case DATABASE:
          saveDatabaseMeta(element, versionComment);
          break;
        case SLAVE_SERVER:
          saveSlaveServer(element, versionComment);
          break;
        //        case CLUSTER_SCHEMA:
        //          slaveDelegate.saveClusterSchema(element, versionComment, monitor);
        //          break;
        case PARTITION_SCHEMA:
          savePartitionSchema(element, versionComment);
          break;
        default:
          throw new KettleException("It's not possible to save Class [" + element.getClass().getName()
              + "] to the repository");
      }
    } catch (Exception e) {
      throw new KettleException("Unable to save repository element [" + element + "]", e);
    }
  }

  private void saveJob(final RepositoryElementInterface element, final String versionComment) throws KettleException {
    jobDelegate.saveSharedObjects(element, versionComment);

    boolean isUpdate = element.getObjectId() != null;
    RepositoryFile file = null;
    if (isUpdate) {
      file = pur.getFileById(element.getObjectId().getId());
      // update description
      file = new RepositoryFile.Builder(file).description(element.getDescription()).build();
      file = pur.updateFile(file, new NodeRepositoryFileData(jobDelegate.elementToDataNode(element)), versionComment);
    } else {
      file = new RepositoryFile.Builder(element.getName() + RepositoryObjectType.JOB.getExtension()).versioned(true)
          .description(element.getDescription()).build();
      file = pur.createFile(pur.getFileById(element.getRepositoryDirectory().getObjectId().getId()).getId(), file,
          new NodeRepositoryFileData(jobDelegate.elementToDataNode(element)), versionComment);
    }
    // side effects
    ObjectId objectId = new StringObjectId(file.getId().toString());
    element.setObjectId(objectId);
    element.setObjectRevision(getObjectRevision(objectId, null));
    element.clearChanged();
  }

  protected void saveTrans(final RepositoryElementInterface element, final String versionComment)
      throws KettleException {
    transDelegate.saveSharedObjects(element, versionComment);

    boolean isUpdate = element.getObjectId() != null;
    RepositoryFile file = null;
    if (isUpdate) {
      file = pur.getFileById(element.getObjectId().getId());
      // update description
      file = new RepositoryFile.Builder(file).description(element.getDescription()).build();
      file = pur.updateFile(file, new NodeRepositoryFileData(transDelegate.elementToDataNode(element)), versionComment);
    } else {
      file = new RepositoryFile.Builder(element.getName() + RepositoryObjectType.TRANSFORMATION.getExtension())
          .versioned(true).description(element.getDescription()).build();
      file = pur.createFile(pur.getFileById(element.getRepositoryDirectory().getObjectId().getId()).getId(), file,
          new NodeRepositoryFileData(transDelegate.elementToDataNode(element)), versionComment);
    }
    // side effects
    ObjectId objectId = new StringObjectId(file.getId().toString());
    element.setObjectId(objectId);
    element.setObjectRevision(getObjectRevision(objectId, null));
    element.clearChanged();
  }

  protected void saveDatabaseMeta(final RepositoryElementInterface element, final String versionComment)
      throws KettleException {
    boolean isUpdate = element.getObjectId() != null;
    RepositoryFile file = null;
    if (isUpdate) {
      file = pur.getFileById(element.getObjectId().getId());
      file = pur.updateFile(file, new NodeRepositoryFileData(databaseMetaTransformer.elementToDataNode(element)),
          versionComment);
    } else {
      file = new RepositoryFile.Builder(element.getName() + RepositoryObjectType.DATABASE.getExtension()).versioned(
          VERSION_SHARED_OBJECTS).build();
      file = pur.createFile(pur.getFile(getDatabaseMetaParentFolderPath()).getId(), file, new NodeRepositoryFileData(
          databaseMetaTransformer.elementToDataNode(element)), versionComment);
    }
    // side effects
    ObjectId objectId = new StringObjectId(file.getId().toString());
    element.setObjectId(objectId);
    element.setObjectRevision(getObjectRevision(objectId, null));
    element.clearChanged();
  }

  public DatabaseMeta loadDatabaseMeta(final ObjectId databaseId, final String versionId) throws KettleException {
    try {
      DatabaseMeta databaseMeta = new DatabaseMeta();
      databaseMetaTransformer.dataNodeToElement(pur.getDataForRead(databaseId.getId(), versionId,
          NodeRepositoryFileData.class).getNode(), databaseMeta);
      databaseMeta.setObjectId(new StringObjectId(databaseId));
      databaseMeta.setObjectRevision(getObjectRevision(databaseId, versionId));
      databaseMeta.clearChanged();
      return databaseMeta;
    } catch (Exception e) {
      throw new KettleException("Unable to load database with id [" + databaseId + "]", e);
    }
  }

  public TransMeta loadTransformation(final String transName, final RepositoryDirectory parentDir,
      final ProgressMonitorListener monitor, final boolean setInternalVariables, final String versionId)
      throws KettleException {
    String absPath = null;
    try {
      absPath = getPath(transName, parentDir, RepositoryObjectType.TRANSFORMATION);
      RepositoryFile file = pur.getFile(absPath);
      TransMeta transMeta = new TransMeta();
      transMeta.setObjectId(new StringObjectId(file.getId().toString()));
      transMeta.setObjectRevision(getObjectRevision(new StringObjectId(file.getId().toString()), versionId));
      transMeta.setRepositoryDirectory(parentDir);
      transMeta.setRepositoryLock(getLock(file));
      transDelegate.loadSharedObjects(transMeta);
      transDelegate.dataNodeToElement(pur.getDataForRead(file.getId(), versionId, NodeRepositoryFileData.class)
          .getNode(), transMeta);
      transMeta.clearChanged();
      return transMeta;
    } catch (Exception e) {
      throw new KettleException("Unable to load transformation from path [" + absPath + "]", e);
    }
  }

  public JobMeta loadJob(String jobname, RepositoryDirectory parentDir, ProgressMonitorListener monitor,
      String versionId) throws KettleException {
    String absPath = null;
    try {
      absPath = getPath(jobname, parentDir, RepositoryObjectType.JOB);
      RepositoryFile file = pur.getFile(absPath);
      JobMeta jobMeta = new JobMeta();
      jobMeta.setObjectId(new StringObjectId(file.getId().toString()));
      jobMeta.setObjectRevision(getObjectRevision(new StringObjectId(file.getId().toString()), versionId));
      jobMeta.setRepositoryDirectory(parentDir);
      jobMeta.setRepositoryLock(getLock(file));
      jobDelegate.loadSharedObjects(jobMeta);
      jobDelegate.dataNodeToElement(
          pur.getDataForRead(file.getId(), versionId, NodeRepositoryFileData.class).getNode(), jobMeta);
      jobMeta.clearChanged();
      return jobMeta;
    } catch (Exception e) {
      throw new KettleException("Unable to load transformation from path [" + absPath + "]", e);
    }
  }

  protected RepositoryLock getLock(final RepositoryFile file) throws KettleException {
    if (file.isLocked()) {
      return new RepositoryLock(new StringObjectId(file.getId().toString()), file.getLockMessage(),
          file.getLockOwner(), file.getLockOwner(), file.getLockDate());
    } else {
      return null;
    }
  }

  protected void savePartitionSchema(final RepositoryElementInterface element, final String versionComment) {
    boolean isUpdate = element.getObjectId() != null;
    RepositoryFile file = null;
    try {
      if (isUpdate) {
        file = pur.getFileById(element.getObjectId().getId());
        file = pur.updateFile(file, new NodeRepositoryFileData(partitionSchemaTransformer.elementToDataNode(element)),
            versionComment);
      } else {
        file = new RepositoryFile.Builder(element.getName() + RepositoryObjectType.PARTITION_SCHEMA.getExtension())
            .versioned(VERSION_SHARED_OBJECTS).build();
        file = pur.createFile(pur.getFile(getPartitionSchemaParentFolderPath()).getId(), file,
            new NodeRepositoryFileData(partitionSchemaTransformer.elementToDataNode(element)), versionComment);
      }
      // side effects
      ObjectId objectId = new StringObjectId(file.getId().toString());
      element.setObjectId(objectId);
      element.setObjectRevision(getObjectRevision(objectId, null));
      element.clearChanged();
    } catch (KettleException ke) {
      ke.printStackTrace();
    }
  }

  protected void saveSlaveServer(final RepositoryElementInterface element, final String versionComment) {
    boolean isUpdate = element.getObjectId() != null;
    RepositoryFile file = null;
    try {
      if (isUpdate) {
        file = pur.getFileById(element.getObjectId().getId());
        file = pur.updateFile(file, new NodeRepositoryFileData(slaveTransformer.elementToDataNode(element)),
            versionComment);
      } else {
        file = new RepositoryFile.Builder(element.getName() + RepositoryObjectType.SLAVE_SERVER.getExtension())
            .versioned(VERSION_SHARED_OBJECTS).build();
        file = pur.createFile(pur.getFile(getSlaveParentFolderPath()).getId(), file,
            new NodeRepositoryFileData(slaveTransformer.elementToDataNode(element)), versionComment);
      }
      // side effects
      ObjectId objectId = new StringObjectId(file.getId().toString());
      element.setObjectId(objectId);
      element.setObjectRevision(getObjectRevision(objectId, null));
      element.clearChanged();
    } catch (KettleException ke) {
      ke.printStackTrace();
    }
    
  }
  //  private RepositoryDirectory getRepositoryDirectory(final ObjectId elementId) throws KettleException {
  //    RepositoryFile file = pur.getFileById(elementId.getId());
  //    RepositoryFile parentFolder = pur.getFileById(file.getParentId());
  //    return loadRepositoryDirectoryTree().findDirectory(parentFolder.getAbsolutePath());
  //  }

  private ObjectRevision getObjectRevision(final ObjectId elementId, final String versionId) {
    VersionSummary versionSummary = pur.getVersionSummary(elementId.getId(), versionId);
    return new PurObjectRevision(versionSummary.getId(), versionSummary.getAuthor(), versionSummary.getDate(),
        versionSummary.getMessage());
  }

  private String getDatabaseMetaParentFolderPath() {
    return RepositoryPaths.getTenantPublicFolderPath();
  }

  private String getPartitionSchemaParentFolderPath() {
    return RepositoryPaths.getTenantPublicFolderPath();
  }

  private String getSlaveParentFolderPath() {
    return RepositoryPaths.getTenantPublicFolderPath();
  }
  
  public void saveConditionStepAttribute(ObjectId idTransformation, ObjectId idStep, String code, Condition condition)
      throws KettleException {

  }

  public void saveDatabaseMetaJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, String nameCode, String idCode,
      DatabaseMeta database) throws KettleException {

    // TODO Auto-generated method stub 

  }

  public void saveDatabaseMetaStepAttribute(ObjectId idTransformation, ObjectId idStep, String code,
      DatabaseMeta database) throws KettleException {

    // TODO Auto-generated method stub 

  }

  public void saveJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, String code, String value)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void saveJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, String code, boolean value)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void saveJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, String code, long value)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void saveJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, int nr, String code, String value)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void saveJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, int nr, String code, boolean value)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void saveJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, int nr, String code, long value)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, String code, String value)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, String code, boolean value)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, String code, long value)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, String code, double value)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, int nr, String code, String value)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, int nr, String code, boolean value)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, int nr, String code, long value)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, int nr, String code, double value)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void undeleteObject(RepositoryElementLocationInterface element) throws KettleException {

    // TODO Auto-generated method stub 

  }

  public void unlockJob(ObjectId idJob) throws KettleException {
    unlockFileById(idJob);
  }

  public void unlockTransformation(ObjectId idTransformation) throws KettleException {
    unlockFileById(idTransformation);
  }

  protected void unlockFileById(ObjectId id) throws KettleException {
    pur.unlockFile(id.getId());
  }

}
