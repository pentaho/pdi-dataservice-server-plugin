package org.pentaho.di.repository.pur;

import java.io.Serializable;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.cluster.ClusterSchema;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ProgressMonitorListener;
import org.pentaho.di.core.annotations.RepositoryPlugin;
import org.pentaho.di.core.changed.ChangedFlagInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleSecurityException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.partition.PartitionSchema;
import org.pentaho.di.repository.IRepositoryExporter;
import org.pentaho.di.repository.IRepositoryImporter;
import org.pentaho.di.repository.IRepositoryService;
import org.pentaho.di.repository.IUser;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.ObjectRecipient;
import org.pentaho.di.repository.ObjectRecipient.Type;
import org.pentaho.di.repository.ObjectRevision;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.repository.RepositoryElementMetaInterface;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.RepositoryObject;
import org.pentaho.di.repository.RepositoryObjectInterface;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.repository.RepositorySecurityManager;
import org.pentaho.di.repository.RepositorySecurityProvider;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.repository.pur.model.EEJobMeta;
import org.pentaho.di.repository.pur.model.EERepositoryObject;
import org.pentaho.di.repository.pur.model.EETransMeta;
import org.pentaho.di.repository.pur.model.EEUserInfo;
import org.pentaho.di.repository.pur.model.ObjectAce;
import org.pentaho.di.repository.pur.model.ObjectAcl;
import org.pentaho.di.repository.pur.model.ObjectPermission;
import org.pentaho.di.repository.pur.model.RepositoryLock;
import org.pentaho.di.repository.pur.model.RepositoryObjectAce;
import org.pentaho.di.repository.pur.model.RepositoryObjectAcl;
import org.pentaho.di.repository.pur.model.RepositoryObjectRecipient;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.repository.pur.services.IAbsSecurityManager;
import org.pentaho.di.ui.repository.pur.services.IAbsSecurityProvider;
import org.pentaho.di.ui.repository.pur.services.IAclService;
import org.pentaho.di.ui.repository.pur.services.ILockService;
import org.pentaho.di.ui.repository.pur.services.IRevisionService;
import org.pentaho.di.ui.repository.pur.services.IRoleSupportSecurityManager;
import org.pentaho.di.ui.repository.pur.services.ITrashService;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.api.repository2.unified.RepositoryFileAce;
import org.pentaho.platform.api.repository2.unified.RepositoryFileAcl;
import org.pentaho.platform.api.repository2.unified.RepositoryFilePermission;
import org.pentaho.platform.api.repository2.unified.RepositoryFileSid;
import org.pentaho.platform.api.repository2.unified.RepositoryFileTree;
import org.pentaho.platform.api.repository2.unified.VersionSummary;
import org.pentaho.platform.api.repository2.unified.data.node.NodeRepositoryFileData;
import org.pentaho.platform.engine.core.system.PentahoSessionHolder;
import org.pentaho.platform.engine.core.system.PentahoSystem;
import org.pentaho.platform.repository2.ClientRepositoryPaths;
import org.pentaho.platform.repository2.unified.webservices.jaxws.IUnifiedRepositoryJaxwsWebService;
import org.pentaho.platform.repository2.unified.webservices.jaxws.UnifiedRepositoryToWebServiceAdapter;

import com.pentaho.commons.dsc.PentahoDscContent;
import com.pentaho.commons.dsc.PentahoLicenseVerifier;
import com.pentaho.commons.dsc.params.KParam;
import com.pentaho.pdi.ws.IRepositorySyncWebService;
import com.pentaho.pdi.ws.RepositorySyncException;

/**
 * Implementation of {@link Repository} that delegates to the Pentaho unified repository (PUR), an instance of
 * {@link IUnifiedRepository}.
 * 
 * @author Matt
 * @author mlowery
 */
@RepositoryPlugin(id = "PentahoEnterpriseRepository", name = "Enterprise Repository", description = "i18n:org.pentaho.di.ui.repository.pur:RepositoryType.Description.EnterpriseRepository", metaClass = "org.pentaho.di.repository.pur.PurRepositoryMeta")
public class PurRepository implements Repository, IRevisionService, IAclService, ITrashService, ILockService {

  // ~ Static fields/initializers ======================================================================================

  // private static final Log logger = LogFactory.getLog(PurRepository.class);

  private static final String REPOSITORY_VERSION = "1.0"; //$NON-NLS-1$

  private static final boolean VERSION_SHARED_OBJECTS = true;

  private static final String FOLDER_PDI = "pdi"; //$NON-NLS-1$

  private static final String FOLDER_PARTITION_SCHEMAS = "partitionSchemas"; //$NON-NLS-1$

  private static final String FOLDER_CLUSTER_SCHEMAS = "clusterSchemas"; //$NON-NLS-1$

  private static final String FOLDER_SLAVE_SERVERS = "slaveServers"; //$NON-NLS-1$

  private static final String FOLDER_DATABASES = "databases"; //$NON-NLS-1$

  // ~ Instance fields =================================================================================================

  /**
   * Indicates that this code should be run in unit test mode (where PUR is passed in instead of created inside this 
   * class).
   */
  private boolean test = false;
  
  private IUnifiedRepository pur;

  private IUser user;

  private PurRepositoryMeta repositoryMeta;

  private DatabaseDelegate databaseMetaTransformer = new DatabaseDelegate(this);

  private PartitionDelegate partitionSchemaTransformer = new PartitionDelegate(this);

  private SlaveDelegate slaveTransformer = new SlaveDelegate(this);

  private ClusterDelegate clusterTransformer = new ClusterDelegate(this);

  private ISharedObjectsTransformer transDelegate = new TransDelegate(this);

  private ISharedObjectsTransformer jobDelegate = new JobDelegate(this);

  private Map<RepositoryObjectType, SharedObjectAssembler<?>> sharedObjectAssemblerMap;

  private RepositorySecurityManager securityManager;

  private RepositorySecurityProvider securityProvider;

  protected LogChannelInterface log;

  protected Serializable cachedSlaveServerParentFolderId;

  protected Serializable cachedPartitionSchemaParentFolderId;

  protected Serializable cachedClusterSchemaParentFolderId;

  protected Serializable cachedDatabaseMetaParentFolderId;

  /**
   * We cache the root directory of the loaded tree, to save retrievals when the findDirectory() method 
   * is called.
   */
  private SoftReference<RepositoryDirectoryInterface> rootRef = null;

  private Map<RepositoryObjectType, List<? extends SharedObjectInterface>> sharedObjectsByType = null;

  private Map<Class<? extends IRepositoryService>, IRepositoryService> serviceMap;

  private List<Class<? extends IRepositoryService>> serviceList;
  
  private boolean connected = false;

  private String connectMessage = null;
  
  // ~ Constructors ====================================================================================================

  public PurRepository() {
    super();
    initSharedObjectAssemblerMap();
  }

  // ~ Methods =========================================================================================================

  protected RepositoryDirectoryInterface getRootDir() throws KettleException {
    if (rootRef != null && rootRef.get() != null) {
      return rootRef.get();
    } else {
      return loadRepositoryDirectoryTree();
    }
  }

  /**
   * Protected for unit tests.
   */
  protected void setTest(final IUnifiedRepository pur) {
    this.pur = pur;
    // set this to avoid NPE in connect()
    this.repositoryMeta.setRepositoryLocation(new PurRepositoryLocation("doesnotmatch"));
    this.test = true;
  }

  public void init(final RepositoryMeta repositoryMeta) {
    this.log = new LogChannel(this);
    this.repositoryMeta = (PurRepositoryMeta) repositoryMeta;
    this.serviceMap = new HashMap<Class<? extends IRepositoryService>, IRepositoryService>();
    this.serviceList = new ArrayList<Class<? extends IRepositoryService>>();
  }

  public void connectInProcess() throws KettleException, KettleSecurityException {
    // connect to the IUnifiedRepository through PentahoSystem
    // this assumes we're running in a BI Platform
    if (!isTest()) {
      String username = PentahoSessionHolder.getSession().getName();
      IUser user1 = new EEUserInfo();
      user1.setLogin(username);
      user1.setName(username);
      this.user = user1;
      pur = PentahoSystem.get(IUnifiedRepository.class);
    }
    // for now, there is no need to support the security manager
    // what about security provider?
  }

  public void connect(String username, String password) throws KettleException, KettleSecurityException {
    try {
      /*
      Three scenarios:
      1. Connect in process: username fetched using PentahoSessionHolder; no authentication occurs
      2. Connect externally with trust: username specified is assumed authenticated if IP of calling code is trusted
      3. Connect externally: authentication occurs normally (i.e. password is checked)
      */
      IUser user1 = new EEUserInfo();
      user1.setLogin(username);
      user1.setPassword(password);
      user1.setName(username);
      this.user = user1;
      
      if (!isTest()) {
        if (PentahoSystem.getApplicationContext() != null) {
          if (PentahoSystem.getApplicationContext().getBaseUrl() != null) {
            String repoUrl = repositoryMeta.getRepositoryLocation().getUrl();
            String baseUrl = PentahoSystem.getApplicationContext().getBaseUrl();
            if (repoUrl.endsWith("/")) {
              repoUrl = repoUrl.substring(0, repoUrl.length() - 1);
            }
            if (baseUrl.endsWith("/")) {
              baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
            }
            if (repoUrl.startsWith(baseUrl)) {
              connectInProcess();
              connected = true;
              return;
            }
          }
        }

        IRepositorySyncWebService syncWebService = WsFactory.createService(repositoryMeta, "repositorySync", //$NON-NLS-1$
            username, password, IRepositorySyncWebService.class);
        
        try {
          syncWebService.sync(repositoryMeta.getName(), repositoryMeta.getRepositoryLocation().getUrl());
        } catch (RepositorySyncException e) {
          log.logError(e.getMessage(), e);
          // this message will be presented to the user in spoon
          connectMessage = e.getMessage();
        }
        
        IUnifiedRepositoryJaxwsWebService repoWebService = WsFactory.createService(repositoryMeta, "unifiedRepository", //$NON-NLS-1$
            username, password, IUnifiedRepositoryJaxwsWebService.class);

        pur = new UnifiedRepositoryToWebServiceAdapter(repoWebService);

        // We need to add the service class in the list in the order of dependencies
        // IRoleSupportSecurityManager depends RepositorySecurityManager to be present
        securityProvider = new AbsSecurityProvider(this, this.repositoryMeta, user);
        registerRepositoryService(RepositorySecurityProvider.class, securityProvider);
        registerRepositoryService(IAbsSecurityProvider.class, securityProvider);
        // If the user does not have access to administer security we do not
        // need to added them to the service list
        if (allowedActionsContains((AbsSecurityProvider) securityProvider,
            IAbsSecurityProvider.ADMINISTER_SECURITY_ACTION)) {
          securityManager = new AbsSecurityManager(this, this.repositoryMeta, user);
          // Set the reference of the security manager to security provider for user role list change event
          ((PurRepositorySecurityProvider) securityProvider)
              .setUserRoleDelegate(((PurRepositorySecurityManager) securityManager).getUserRoleDelegate());

          registerRepositoryService(RepositorySecurityManager.class, securityManager);
          registerRepositoryService(IRoleSupportSecurityManager.class, securityManager);
          registerRepositoryService(IAbsSecurityManager.class, securityManager);
        }
        registerRepositoryService(IRevisionService.class, this);
        registerRepositoryService(IAclService.class, this);
        registerRepositoryService(ITrashService.class, this);
        registerRepositoryService(ILockService.class, this);
      }
      connected = true;
    } catch (Throwable e) {
      connected = false;
      WsFactory.clearServices();
      throw new KettleException(e);
    }
  }

  private boolean isTest() {
    return test;
  }

  /**
   * Add the repository service to the map and add the interface to the list
   * @param clazz
   * @param repositoryService
   */
  private void registerRepositoryService(Class<? extends IRepositoryService> clazz, IRepositoryService repositoryService) {
    this.serviceMap.put(clazz, repositoryService);
    this.serviceList.add(clazz);
  }

  private boolean allowedActionsContains(AbsSecurityProvider provider, String action) throws KettleException {
    List<String> allowedActions = provider.getAllowedActions(IAbsSecurityProvider.NAMESPACE);
    for (String actionName : allowedActions) {
      if (action != null && action.equals(actionName)) {
        return true;
      }
    }
    return false;
  }

  public boolean isConnected() {
    return connected;
  }

  public void disconnect() {
    connected = false;
    WsFactory.clearServices();
  }

  public int countNrJobEntryAttributes(ObjectId idJobentry, String code) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public int countNrStepAttributes(ObjectId idStep, String code) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public RepositoryDirectoryInterface createRepositoryDirectory(final RepositoryDirectoryInterface parentDirectory,
      final String directoryPath) throws KettleException {
    try {
      PentahoDscContent dscContent = PentahoLicenseVerifier.verify(new KParam(PurRepositoryMeta.BUNDLE_REF_NAME));      
      RepositoryDirectoryInterface refreshedParentDir = findDirectory(parentDirectory.getPath());
      // update the passed in repository directory with the children recently loaded from the repo
      parentDirectory.setChildren(refreshedParentDir.getChildren());
      String[] path = Const.splitPath(directoryPath, RepositoryDirectory.DIRECTORY_SEPARATOR);

      RepositoryDirectoryInterface follow = refreshedParentDir;

      for (int level = 0; level < path.length; level++) {
        RepositoryDirectoryInterface child = follow.findChild(path[level]);
        if (child == null) {
          // create this one
          //
          child = new RepositoryDirectory(follow, path[level]);
          if (dscContent.getHolder() != null) {
            saveRepositoryDirectory(child);
            // link this with the parent directory
            follow.addSubdirectory(child);
          }
        }

        follow = child;
      }
      return follow;
    } catch (Exception e) {
      throw new KettleException("Unable to create directory with path [" + directoryPath + "]", e);
    }
  }

  public void saveRepositoryDirectory(final RepositoryDirectoryInterface dir) throws KettleException {
    try {
      PentahoDscContent dscContent = PentahoLicenseVerifier.verify(new KParam(PurRepositoryMeta.BUNDLE_REF_NAME));
      if (dscContent.getSubject() != null) {
        // id of root dir is null--check for it
        RepositoryFile newFolder = pur.createFolder(dir.getParent().getObjectId() != null ? dir.getParent()
            .getObjectId().getId() : null, new RepositoryFile.Builder(dir.getName()).folder(true).build(), null);
        dir.setObjectId(new StringObjectId(newFolder.getId().toString()));
      }
    } catch (Exception e) {
      throw new KettleException("Unable to save repository directory with path [" + getPath(null, dir, null) + "]", e);
    }
  }

  public void deleteRepositoryDirectory(final RepositoryDirectoryInterface dir) throws KettleException {
    try {
      RepositoryFile homeFolder = pur.getFile(ClientRepositoryPaths.getUserHomeFolderPath(user.getLogin()));
      RepositoryFile folder = pur.getFileById(dir.getObjectId().getId());
      if (homeFolder.getId().equals(folder.getId()) || homeFolder.getPath().startsWith(folder.getPath())) {
        throw new KettleException("You are not allowed to delete your home folder.");
      }
      pur.deleteFile(dir.getObjectId().getId(), null);
      rootRef = null;
    } catch (Exception e) {
      throw new KettleException("Unable to delete directory with path [" + getPath(null, dir, null) + "]", e);
    }
  }

  public ObjectId renameRepositoryDirectory(final ObjectId dirId, final RepositoryDirectoryInterface newParent,
      final String newName) throws KettleException {
    // dir ID is used to find orig obj; new parent is used as new parent (might be null meaning no change in parent); 
    // new name is used as new file name (might be null meaning no change in name)
    String finalName = null;
    String finalParentPath = null;
    String interimFolderPath = null;
    try {
      RepositoryFile homeFolder = pur.getFile(ClientRepositoryPaths.getUserHomeFolderPath(user.getLogin()));
      RepositoryFile folder = pur.getFileById(dirId.getId());
      if (homeFolder.getId().equals(dirId.getId()) || homeFolder.getPath().startsWith(folder.getPath())) {
        throw new KettleException("You are not allowed to move/rename your home folder.");
      }
      finalName = (newName != null ? newName : folder.getName());
      interimFolderPath = getParentPath(folder.getPath());
      finalParentPath = (newParent != null ? getPath(null, newParent, null) : interimFolderPath);
      pur.moveFile(dirId.getId(), finalParentPath + RepositoryFile.SEPARATOR + finalName, null);
      rootRef = null;
      return dirId;
    } catch (Exception e) {
      throw new KettleException("Unable to move/rename directory with id [" + dirId + "] to new parent ["
          + finalParentPath + "] and new name [nant" + finalName + "]", e);
    }
  }

  protected RepositoryFileTree loadRepositoryFileTree(String path) {
    return pur.getTree(path, -1, null);
  }

  public RepositoryDirectoryInterface loadRepositoryDirectoryTree() throws KettleException {
    // this method forces a reload of the repository directory tree structure
    // a new rootRef will be obtained - this is a SoftReference which will be used
    // by any calls to getRootDir()
    RepositoryFileTree rootFileTree = loadRepositoryFileTree(ClientRepositoryPaths.getRootFolderPath());
    RepositoryDirectoryInterface rootDir = initRepositoryDirectoryTree(rootFileTree);
    rootRef = new SoftReference<RepositoryDirectoryInterface>(rootDir);
    return rootDir;
  }

  private RepositoryDirectoryInterface initRepositoryDirectoryTree(RepositoryFileTree repoTree) throws KettleException {
    RepositoryFile rootFolder = repoTree.getFile();
    RepositoryDirectory rootDir = new RepositoryDirectory();
    rootDir.setObjectId(new StringObjectId(rootFolder.getId().toString()));
    loadRepositoryDirectory(rootDir, rootFolder, repoTree);

    // Example: /etc
    RepositoryDirectory etcDir = rootDir.findDirectory(ClientRepositoryPaths.getEtcFolderPath());

    RepositoryDirectory newRoot = new RepositoryDirectory();
    newRoot.setObjectId(rootDir.getObjectId());
    newRoot.setVisible(false);

    for (int i = 0; i < rootDir.getNrSubdirectories(); i++) {
      RepositoryDirectory childDir = rootDir.getSubdirectory(i);
      // Don't show /etc
      boolean isEtcChild = childDir.equals(etcDir);
      if (isEtcChild) {
        continue;
      }
      newRoot.addSubdirectory(childDir);
    }
    return newRoot;
  }

  private void loadRepositoryDirectory(final RepositoryDirectoryInterface parentDir, final RepositoryFile folder, final RepositoryFileTree treeNode)
      throws KettleException {
    try {
      List<RepositoryElementMetaInterface> fileChildren = new ArrayList<RepositoryElementMetaInterface>();
      List<RepositoryFileTree> children = treeNode.getChildren();
      if (children != null) {
        for (RepositoryFileTree child : children) {
          if (child.getFile().isFolder()) {
            RepositoryDirectory dir = new RepositoryDirectory(parentDir, child.getFile().getName());
            dir.setObjectId(new StringObjectId(child.getFile().getId().toString()));
            parentDir.addSubdirectory(dir);
            loadRepositoryDirectory(dir, child.getFile(), child);
          } else {
            // a real file, like a Transformation or Job
            RepositoryLock lock = getLock(child.getFile());
            RepositoryObjectType objectType = getObjectType(child.getFile().getName());
            fileChildren.add(new EERepositoryObject(new StringObjectId(child.getFile().getId().toString()), child.getFile().getTitle(), parentDir, null, child.getFile().getLastModifiedDate(),
              objectType, null, lock, false));
          }
        }
        parentDir.setRepositoryObjects(fileChildren);
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
        if (child.isFolder()) {
          childNames.add(child.getName());
        }
      }
      return childNames.toArray(new String[0]);
    } catch (Exception e) {
      throw new KettleException("Unable to get list of object names from directory [" + idDirectory + "]", e);
    }
  }

  public void deleteClusterSchema(ObjectId idCluster) throws KettleException {
    permanentlyDeleteSharedObject(idCluster);
    removeFromSharedObjectCache(RepositoryObjectType.CLUSTER_SCHEMA, idCluster);
  }

  public void deleteJob(ObjectId idJob) throws KettleException {
    deleteFileById(idJob);
  }

  public void deleteJob(ObjectId jobId, String versionId) throws KettleException {
    pur.deleteFileAtVersion(jobId.getId(), versionId);
    rootRef = null;
  }

  protected void permanentlyDeleteSharedObject(final ObjectId id) throws KettleException {
    try {
      pur.deleteFile(id.getId(), true, null);
    } catch (Exception e) {
      throw new KettleException("Unable to delete object with id [" + id + "]", e);
    }    
  }
  
  public void deleteFileById(final ObjectId id) throws KettleException {
    try {
      pur.deleteFile(id.getId(), null);
      rootRef = null;
    } catch (Exception e) {
      throw new KettleException("Unable to delete object with id [" + id + "]", e);
    }
  }

  public void restoreJob(ObjectId idJob, String versionId, String versionComment) {
    pur.restoreFileAtVersion(idJob.getId(), versionId, versionComment);
    rootRef = null;
  }

  public void restoreTransformation(ObjectId idTransformation, String versionId, String versionComment)
      throws KettleException {
    pur.restoreFileAtVersion(idTransformation.getId(), versionId, versionComment);
    rootRef = null;
  }

  public void deletePartitionSchema(ObjectId idPartitionSchema) throws KettleException {
    permanentlyDeleteSharedObject(idPartitionSchema);
    removeFromSharedObjectCache(RepositoryObjectType.PARTITION_SCHEMA, idPartitionSchema);
  }

  public void deleteSlave(ObjectId idSlave) throws KettleException {
    permanentlyDeleteSharedObject(idSlave);
    removeFromSharedObjectCache(RepositoryObjectType.SLAVE_SERVER, idSlave);
  }

  public void deleteTransformation(ObjectId idTransformation) throws KettleException {
    deleteFileById(idTransformation);
    rootRef = null;
  }

  public boolean exists(final String name, final RepositoryDirectoryInterface repositoryDirectory,
      final RepositoryObjectType objectType) throws KettleException {
    try {
      String absPath = getPath(name, repositoryDirectory, objectType);
      return pur.getFile(absPath) != null;
    } catch (Exception e) {
      throw new KettleException("Unable to verify if the repository element [" + name + "] exists in ", e);
    }
  }
  
  private String getPath(final String name, final RepositoryDirectoryInterface repositoryDirectory,
      final RepositoryObjectType objectType) {

    String path = null;

    // need to check for null id since shared objects return a non-null repoDir (see partSchema.getRepositoryDirectory())
    if (repositoryDirectory != null && repositoryDirectory.getObjectId() != null) {
      path = repositoryDirectory.getPath();
    }

    // return the directory path
    if (objectType == null) {
      return path;
    }

    String sanitizedName = checkAndSanitize(name);
    
    switch (objectType) {
      case DATABASE: {
        return getDatabaseMetaParentFolderPath() + RepositoryFile.SEPARATOR + sanitizedName
            + RepositoryObjectType.DATABASE.getExtension();
      }
      case TRANSFORMATION: {
        return path + (path.endsWith(RepositoryFile.SEPARATOR)?"":RepositoryFile.SEPARATOR) + sanitizedName + RepositoryObjectType.TRANSFORMATION.getExtension();
      }
      case PARTITION_SCHEMA: {
        return getPartitionSchemaParentFolderPath() + RepositoryFile.SEPARATOR + sanitizedName
            + RepositoryObjectType.PARTITION_SCHEMA.getExtension();
      }
      case SLAVE_SERVER: {
        return getSlaveServerParentFolderPath() + RepositoryFile.SEPARATOR + sanitizedName
            + RepositoryObjectType.SLAVE_SERVER.getExtension();
      }
      case CLUSTER_SCHEMA: {
        return getClusterSchemaParentFolderPath() + RepositoryFile.SEPARATOR + sanitizedName
            + RepositoryObjectType.CLUSTER_SCHEMA.getExtension();
      }
      case JOB: {
        return path + (path.endsWith(RepositoryFile.SEPARATOR)?"":RepositoryFile.SEPARATOR) + sanitizedName + RepositoryObjectType.JOB.getExtension();
      }
      default: {
        throw new UnsupportedOperationException("not implemented");
      }
    }
  }

  public ObjectId getClusterID(String name) throws KettleException {
    try {
      return getObjectId(name, null, RepositoryObjectType.CLUSTER_SCHEMA, false);
    } catch (Exception e) {
      throw new KettleException("Unable to get ID for cluster schema [" + name + "]", e);
    }
  }

  public ObjectId[] getClusterIDs(boolean includeDeleted) throws KettleException {
    try {
      List<RepositoryFile> children = getAllFilesOfType(null, RepositoryObjectType.CLUSTER_SCHEMA, includeDeleted);
      List<ObjectId> ids = new ArrayList<ObjectId>();
      for (RepositoryFile file : children) {
        ids.add(new StringObjectId(file.getId().toString()));
      }
      return ids.toArray(new ObjectId[0]);
    } catch (Exception e) {
      throw new KettleException("Unable to get all cluster schema IDs", e);
    }
  }

  public String[] getClusterNames(boolean includeDeleted) throws KettleException {
    try {
      List<RepositoryFile> children = getAllFilesOfType(null, RepositoryObjectType.CLUSTER_SCHEMA, includeDeleted);
      List<String> names = new ArrayList<String>();
      for (RepositoryFile file : children) {
        names.add(file.getTitle());
      }
      return names.toArray(new String[0]);
    } catch (Exception e) {
      throw new KettleException("Unable to get all cluster schema names", e);
    }
  }

  public ObjectId getDatabaseID(final String name) throws KettleException {
    try {
      return getObjectId(name, null, RepositoryObjectType.DATABASE, false);
    } catch (Exception e) {
      throw new KettleException("Unable to get ID for database [" + name + "]", e);
    }
  }

  /**
   * Copying the behavior of the original JCRRepository, this implementation returns IDs of deleted objects too.
   */
  private ObjectId getObjectId(final String name, final RepositoryDirectoryInterface dir, final RepositoryObjectType objectType,
      boolean includedDeleteFiles) {
    final String absPath = getPath(name, dir, objectType);
    RepositoryFile file = pur.getFile(absPath);
    if (file != null) {
      // file exists
      return new StringObjectId(file.getId().toString());
    } else if (includedDeleteFiles) {
      switch (objectType) {
        case DATABASE: {
          // file either never existed or has been deleted
          List<RepositoryFile> deletedChildren = pur.getDeletedFiles(getDatabaseMetaParentFolderId(), name
              + RepositoryObjectType.DATABASE.getExtension());
          if (!deletedChildren.isEmpty()) {
            return new StringObjectId(deletedChildren.get(0).getId().toString());
          } else {
            return null;
          }
        }
        case TRANSFORMATION: {
          // file either never existed or has been deleted
          List<RepositoryFile> deletedChildren = pur.getDeletedFiles(dir.getObjectId().getId(), name
              + RepositoryObjectType.TRANSFORMATION.getExtension());
          if (!deletedChildren.isEmpty()) {
            return new StringObjectId(deletedChildren.get(0).getId().toString());
          } else {
            return null;
          }
        }
        case PARTITION_SCHEMA: {
          // file either never existed or has been deleted
          List<RepositoryFile> deletedChildren = pur.getDeletedFiles(getPartitionSchemaParentFolderId(), name
              + RepositoryObjectType.PARTITION_SCHEMA.getExtension());
          if (!deletedChildren.isEmpty()) {
            return new StringObjectId(deletedChildren.get(0).getId().toString());
          } else {
            return null;
          }
        }
        case SLAVE_SERVER: {
          // file either never existed or has been deleted
          List<RepositoryFile> deletedChildren = pur.getDeletedFiles(getSlaveServerParentFolderId(), name
              + RepositoryObjectType.SLAVE_SERVER.getExtension());
          if (!deletedChildren.isEmpty()) {
            return new StringObjectId(deletedChildren.get(0).getId().toString());
          } else {
            return null;
          }
        }
        case CLUSTER_SCHEMA: {
          // file either never existed or has been deleted
          List<RepositoryFile> deletedChildren = pur.getDeletedFiles(getClusterSchemaParentFolderId(), name
              + RepositoryObjectType.CLUSTER_SCHEMA.getExtension());
          if (!deletedChildren.isEmpty()) {
            return new StringObjectId(deletedChildren.get(0).getId().toString());
          } else {
            return null;
          }
        }
        case JOB: {
          // file either never existed or has been deleted
          List<RepositoryFile> deletedChildren = pur.getDeletedFiles(dir.getObjectId().getId(), name
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
    } else {
      return null;
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

  protected List<RepositoryFile> getAllFilesOfType(final ObjectId dirId, final RepositoryObjectType objectType,
      final boolean includeDeleted) throws KettleException {
    return getAllFilesOfType(dirId, Arrays.asList(new RepositoryObjectType[] { objectType }), includeDeleted);
  }

  protected List<RepositoryFile> getAllFilesOfType(final ObjectId dirId, final List<RepositoryObjectType> objectTypes,
      final boolean includeDeleted) throws KettleException {

    List<RepositoryFile> allChildren = new ArrayList<RepositoryFile>();
    List<RepositoryFile> children = getAllFilesOfType(dirId, objectTypes);
    allChildren.addAll(children);
    if (includeDeleted) {
      List<RepositoryFile> deletedChildren = getAllDeletedFilesOfType(dirId, objectTypes);
      allChildren.addAll(deletedChildren);
      Collections.sort(allChildren);
    }
    return allChildren;
  }

  protected List<RepositoryFile> getAllFilesOfType(final ObjectId dirId, final List<RepositoryObjectType> objectTypes)
      throws KettleException {
    Set<Serializable> parentFolderIds = new HashSet<Serializable>();
    List<String> filters = new ArrayList<String>();
    for (RepositoryObjectType objectType : objectTypes) {
      switch (objectType) {
        case DATABASE: {
          parentFolderIds.add(getDatabaseMetaParentFolderId());
          filters.add("*" + RepositoryObjectType.DATABASE.getExtension()); //$NON-NLS-1$
          break;
        }
        case TRANSFORMATION: {
          parentFolderIds.add(dirId.getId());
          filters.add("*" + RepositoryObjectType.TRANSFORMATION.getExtension()); //$NON-NLS-1$
          break;
        }
        case PARTITION_SCHEMA: {
          parentFolderIds.add(getPartitionSchemaParentFolderId());
          filters.add("*" + RepositoryObjectType.PARTITION_SCHEMA.getExtension()); //$NON-NLS-1$
          break;
        }
        case SLAVE_SERVER: {
          parentFolderIds.add(getSlaveServerParentFolderId());
          filters.add("*" + RepositoryObjectType.SLAVE_SERVER.getExtension()); //$NON-NLS-1$
          break;
        }
        case CLUSTER_SCHEMA: {
          parentFolderIds.add(getClusterSchemaParentFolderId());
          filters.add("*" + RepositoryObjectType.CLUSTER_SCHEMA.getExtension()); //$NON-NLS-1$
          break;
        }
        case JOB: {
          parentFolderIds.add(dirId.getId());
          filters.add("*" + RepositoryObjectType.JOB.getExtension()); //$NON-NLS-1$
          break;
        }
        default: {
          throw new UnsupportedOperationException("not implemented");
        }
      }
    }
    StringBuilder mergedFilterBuf = new StringBuilder();
    // build filter
    int i = 0;
    for (String filter : filters) {
      if (i++ > 0) {
        mergedFilterBuf.append(" | "); //$NON-NLS-1$
      }
      mergedFilterBuf.append(filter);
    }
    List<RepositoryFile> allFiles = new ArrayList<RepositoryFile>();
    for (Serializable parentFolderId : parentFolderIds) {
      allFiles.addAll(pur.getChildren(parentFolderId, mergedFilterBuf.toString()));
    }
    Collections.sort(allFiles);
    return allFiles;
  }

  protected List<RepositoryFile> getAllDeletedFilesOfType(final ObjectId dirId,
      final List<RepositoryObjectType> objectTypes) throws KettleException {
    Set<Serializable> parentFolderIds = new HashSet<Serializable>();
    List<String> filters = new ArrayList<String>();
    for (RepositoryObjectType objectType : objectTypes) {
      switch (objectType) {
        case DATABASE: {
          parentFolderIds.add(getDatabaseMetaParentFolderId());
          filters.add("*" + RepositoryObjectType.DATABASE.getExtension()); //$NON-NLS-1$
          break;
        }
        case TRANSFORMATION: {
          parentFolderIds.add(dirId.getId());
          filters.add("*" + RepositoryObjectType.TRANSFORMATION.getExtension()); //$NON-NLS-1$
          break;
        }
        case PARTITION_SCHEMA: {
          parentFolderIds.add(getPartitionSchemaParentFolderId());
          filters.add("*" + RepositoryObjectType.PARTITION_SCHEMA.getExtension()); //$NON-NLS-1$
          break;
        }
        case SLAVE_SERVER: {
          parentFolderIds.add(getSlaveServerParentFolderId());
          filters.add("*" + RepositoryObjectType.SLAVE_SERVER.getExtension()); //$NON-NLS-1$
          break;
        }
        case CLUSTER_SCHEMA: {
          parentFolderIds.add(getClusterSchemaParentFolderId());
          filters.add("*" + RepositoryObjectType.CLUSTER_SCHEMA.getExtension()); //$NON-NLS-1$
          break;
        }
        case JOB: {
          parentFolderIds.add(dirId.getId());
          filters.add("*" + RepositoryObjectType.JOB.getExtension()); //$NON-NLS-1$
          break;
        }
        default: {
          throw new UnsupportedOperationException();
        }
      }
    }
    StringBuilder mergedFilterBuf = new StringBuilder();
    // build filter
    int i = 0;
    for (String filter : filters) {
      if (i++ > 0) {
        mergedFilterBuf.append(" | "); //$NON-NLS-1$
      }
      mergedFilterBuf.append(filter);
    }
    List<RepositoryFile> allFiles = new ArrayList<RepositoryFile>();
    for (Serializable parentFolderId : parentFolderIds) {
      allFiles.addAll(pur.getDeletedFiles(parentFolderId, mergedFilterBuf.toString()));
    }
    Collections.sort(allFiles);
    return allFiles;
  }

  public String[] getDatabaseNames(boolean includeDeleted) throws KettleException {
    try {
      List<RepositoryFile> children = getAllFilesOfType(null, RepositoryObjectType.DATABASE, includeDeleted);
      List<String> names = new ArrayList<String>();
      for (RepositoryFile file : children) {
        names.add(file.getTitle());
      }
      return names.toArray(new String[0]);
    } catch (Exception e) {
      throw new KettleException("Unable to get all database names", e);
    }
  }

  /**
   * Initialize the shared object assembler map with known assemblers
   */
  private void initSharedObjectAssemblerMap() {
    sharedObjectAssemblerMap = new HashMap<RepositoryObjectType, SharedObjectAssembler<?>>();
    sharedObjectAssemblerMap.put(RepositoryObjectType.DATABASE, databaseMetaTransformer);
    sharedObjectAssemblerMap.put(RepositoryObjectType.CLUSTER_SCHEMA, clusterTransformer);
    sharedObjectAssemblerMap.put(RepositoryObjectType.PARTITION_SCHEMA, partitionSchemaTransformer);
    sharedObjectAssemblerMap.put(RepositoryObjectType.SLAVE_SERVER, slaveTransformer);
  }

  public void clearSharedObjectCache() {
    sharedObjectsByType = null;
  }
  
  /**
   * Read shared objects of the types provided from the repository.  Every {@link SharedObjectInterface} that is read will be
   * fully loaded as if it has been loaded through {@link #loadDatabaseMeta(ObjectId, String)}, {@link #loadClusterSchema(ObjectId, List, String)}, etc.
   * <p>This method was introduced to reduce the number of server calls for loading shared objects to a constant number: {@code 2 + n, where n is the number of types requested}.</p>
   * 
   * @param sharedObjectsByType Map of type to shared objects.  Each map entry will contain a non-null {@link List} of {@link RepositoryObjectType}s for every type provided.  Only entries for types provided will be altered.
   * @param types Types of repository objects to read from the repository
   * @throws KettleException
   */
  protected void readSharedObjects(Map<RepositoryObjectType, List<? extends SharedObjectInterface>> sharedObjectsByType, RepositoryObjectType... types) throws KettleException {
    // Overview:  
    //  1) We will fetch RepositoryFile, NodeRepositoryFileData, and VersionSummary for all types provided.  
    //  2) We assume that unless an exception is thrown every RepositoryFile returned by getFilesByType(..) have a 
    //     matching NodeRepositoryFileData and VersionSummary.
    //  3) With all files, node data, and versions in hand we will iterate over them, merging them back into usable shared objects
    List<RepositoryFile> allFiles = new ArrayList<RepositoryFile>();
    // Since type is not preserved in the RepositoryFile we fetch files by type so we don't rely on parsing the name to determine type afterward
    // Map must be ordered or we can't match up files with data and version summary
    LinkedHashMap<RepositoryObjectType, List<RepositoryFile>> filesByType = getFilesByType(allFiles, types);
    try
    {
      List<NodeRepositoryFileData> data = pur.getDataForReadInBatch(allFiles, NodeRepositoryFileData.class);
      List<VersionSummary> versions = pur.getVersionSummaryInBatch(allFiles);
      // Only need one iterator for all data and versions.  We will work through them as we process the files by type, in order.
      Iterator<NodeRepositoryFileData> dataIter = data.iterator();
      Iterator<VersionSummary> versionsIter = versions.iterator();
  
      // Assemble into completely loaded SharedObjectInterfaces by type
      for (Entry<RepositoryObjectType, List<RepositoryFile>> entry : filesByType.entrySet()) {
        SharedObjectAssembler<?> assembler = sharedObjectAssemblerMap.get(entry.getKey());
        if (assembler == null) {
          throw new UnsupportedOperationException(String.format("Cannot assemble shared object of type [%s]", entry.getKey())); //$NON-NLS-1$
        }
        // For all files of this type, assemble them from the pieces of data pulled from the repository
        Iterator<RepositoryFile> filesIter = entry.getValue().iterator();
        List<SharedObjectInterface> sharedObjects = new ArrayList<SharedObjectInterface>(entry.getValue().size());
        // Exceptions are thrown during lookup if data or versions aren't found so all the lists should be the same size
        // (no need to check for next on all iterators)
        while (filesIter.hasNext()) {
          RepositoryFile file = filesIter.next();
          NodeRepositoryFileData repoData = dataIter.next();
          VersionSummary version = versionsIter.next();
          sharedObjects.add(assembler.assemble(file, repoData, version));
        }
        sharedObjectsByType.put(entry.getKey(), sharedObjects);
      }
    } catch (Exception ex) {
      // TODO i18n
      throw new KettleException("Unable to load shared objects", ex); //$NON-NLS-1$
    }
  }

  /**
   * Fetch {@link RepositoryFile}s by {@code RepositoryObjectType}.
   * 
   * @param allFiles List to add files into.
   * @param types Types of files to fetch
   * @return Ordered map of object types to list of files.
   * @throws KettleException 
   */
  private LinkedHashMap<RepositoryObjectType, List<RepositoryFile>> getFilesByType(List<RepositoryFile> allFiles,
      RepositoryObjectType... types) throws KettleException {
    // Must be ordered or we can't match up files with data and version summary
    LinkedHashMap<RepositoryObjectType, List<RepositoryFile>> filesByType = new LinkedHashMap<RepositoryObjectType, List<RepositoryFile>>();
    // Since type is not preserved in the RepositoryFile we must fetch files by type
    for (RepositoryObjectType type : types) {
      try {
        List<RepositoryFile> files = getAllFilesOfType(null, type, false);
        filesByType.put(type, files);
        allFiles.addAll(files);
      } catch (Exception ex) {
        // TODO i18n
        throw new KettleException(String.format("Unable to get all files of type [%s]", type), ex); //$NON-NLS-1$
      }
    }
    return filesByType;
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
    RepositoryFile fileToDelete = null;
    try {
      fileToDelete = pur.getFile(getPath(databaseName, null, RepositoryObjectType.DATABASE));
    } catch (Exception e) {
      throw new KettleException("Unable to delete database with name [" + databaseName + "]", e);
    }
    ObjectId idDatabase = new StringObjectId(fileToDelete.getId().toString());
    permanentlyDeleteSharedObject(idDatabase);
    removeFromSharedObjectCache(RepositoryObjectType.DATABASE, idDatabase);
  }

  public boolean getJobEntryAttributeBoolean(ObjectId idJobentry, String code) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public boolean getJobEntryAttributeBoolean(ObjectId idJobentry, int nr, String code) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public boolean getJobEntryAttributeBoolean(ObjectId idJobentry, String code, boolean def) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public long getJobEntryAttributeInteger(ObjectId idJobentry, String code) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public long getJobEntryAttributeInteger(ObjectId idJobentry, int nr, String code) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public String getJobEntryAttributeString(ObjectId idJobentry, String code) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public String getJobEntryAttributeString(ObjectId idJobentry, int nr, String code) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public ObjectId getJobId(final String name, final RepositoryDirectoryInterface repositoryDirectory) throws KettleException {
    try {
      return getObjectId(name, repositoryDirectory, RepositoryObjectType.JOB, false);
    } catch (Exception e) {
      throw new KettleException("Unable to get ID for job [" + name + "]", e);
    }
  }

  public RepositoryLock getJobLock(ObjectId idJob) throws KettleException {
    return getLockById(idJob);
  }

  public String[] getJobNames(ObjectId idDirectory, boolean includeDeleted) throws KettleException {
    try {
      List<RepositoryFile> children = getAllFilesOfType(idDirectory, RepositoryObjectType.JOB, includeDeleted);
      List<String> names = new ArrayList<String>();
      for (RepositoryFile file : children) {
        names.add(file.getTitle());
      }
      return names.toArray(new String[0]);
    } catch (Exception e) {
      throw new KettleException("Unable to get all job names", e);
    }
  }

  public List<RepositoryElementMetaInterface> getJobObjects(ObjectId idDirectory, boolean includeDeleted) throws KettleException {
    return getPdiObjects(idDirectory, Arrays.asList(new RepositoryObjectType[] { RepositoryObjectType.JOB }),
        includeDeleted);
  }

  public LogChannelInterface getLog() {
    return log;
  }

  public String getName() {
    return repositoryMeta.getName();
  }

  public ObjectId getPartitionSchemaID(String name) throws KettleException {
    try {
      return getObjectId(name, null, RepositoryObjectType.PARTITION_SCHEMA, false);
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
        names.add(file.getTitle());
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
   * The implementation of this method is more complex because it takes a {@link RepositoryElementInterface}
   * which does not have an ID.
   */
  public List<ObjectRevision> getRevisions(final RepositoryElementInterface element) throws KettleException {
    return getRevisions(element.getObjectId());
  }

  public RepositorySecurityProvider getSecurityProvider() {
    return securityProvider;
  }

  public RepositorySecurityManager getSecurityManager() {
    return securityManager;
  }

  public ObjectId getSlaveID(String name) throws KettleException {
    try {
      return getObjectId(name, null, RepositoryObjectType.SLAVE_SERVER, false);
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
        names.add(file.getTitle());
      }
      return names.toArray(new String[0]);
    } catch (Exception e) {
      throw new KettleException("Unable to get all slave server names", e);
    }
  }

  @SuppressWarnings("unchecked")
  public List<SlaveServer> getSlaveServers() throws KettleException {
    loadAndCacheSharedObjects();
    return (List<SlaveServer>) sharedObjectsByType.get(RepositoryObjectType.SLAVE_SERVER);
  }

  public boolean getStepAttributeBoolean(ObjectId idStep, String code) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public boolean getStepAttributeBoolean(ObjectId idStep, int nr, String code) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public boolean getStepAttributeBoolean(ObjectId idStep, int nr, String code, boolean def) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public long getStepAttributeInteger(ObjectId idStep, String code) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public long getStepAttributeInteger(ObjectId idStep, int nr, String code) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public String getStepAttributeString(ObjectId idStep, String code) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public String getStepAttributeString(ObjectId idStep, int nr, String code) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public ObjectId getTransformationID(String name, RepositoryDirectoryInterface repositoryDirectory) throws KettleException {
    try {
      return getObjectId(name, repositoryDirectory, RepositoryObjectType.TRANSFORMATION, false);
    } catch (Exception e) {
      throw new KettleException("Unable to get ID for transformation [" + name + "]", e);
    }
  }

  public RepositoryLock getTransformationLock(ObjectId idTransformation) throws KettleException {
    return getLockById(idTransformation);
  }

  protected RepositoryLock getLockById(final ObjectId id) throws KettleException {
    try {
    RepositoryFile file = pur.getFileById(id.getId());
    return getLock(file);
    } catch (Exception e) {
      throw new KettleException("Unable to get lock for object with id [" + id + "]", e);
    }
  }

  public String[] getTransformationNames(ObjectId idDirectory, boolean includeDeleted) throws KettleException {
    try {
      List<RepositoryFile> children = getAllFilesOfType(idDirectory, RepositoryObjectType.TRANSFORMATION,
          includeDeleted);
      List<String> names = new ArrayList<String>();
      for (RepositoryFile file : children) {
        names.add(file.getTitle());
      }
      return names.toArray(new String[0]);
    } catch (Exception e) {
      throw new KettleException("Unable to get all transformation names", e);
    }
  }

  public List<RepositoryElementMetaInterface> getTransformationObjects(ObjectId idDirectory, boolean includeDeleted)
      throws KettleException {
    return getPdiObjects(idDirectory,
        Arrays.asList(new RepositoryObjectType[] { RepositoryObjectType.TRANSFORMATION }), includeDeleted);
  }

  protected List<RepositoryElementMetaInterface> getPdiObjects(ObjectId dirId, List<RepositoryObjectType> objectTypes,
      boolean includeDeleted) throws KettleException {
    try {

      RepositoryDirectoryInterface repDir = getRootDir().findDirectory(dirId);

      List<RepositoryElementMetaInterface> list = new ArrayList<RepositoryElementMetaInterface>();
      List<RepositoryFile> nonDeletedChildren = getAllFilesOfType(dirId, objectTypes);
      for (RepositoryFile file : nonDeletedChildren) {
        RepositoryLock lock = getLock(file);
        RepositoryObjectType objectType = getObjectType(file.getName());
        list.add(new EERepositoryObject(new StringObjectId(file.getId().toString()), file.getTitle(), repDir, null, file.getLastModifiedDate(),
            objectType, null, lock, false));
      }
      if (includeDeleted) {
        List<RepositoryFile> deletedChildren = getAllDeletedFilesOfType(dirId, objectTypes);
        for (RepositoryFile file : deletedChildren) {
          RepositoryLock lock = getLock(file);
          RepositoryObjectType objectType = getObjectType(file.getName());
          list.add(new EERepositoryObject(new StringObjectId(file.getId().toString()), file.getTitle(), repDir, null, file.getLastModifiedDate(),
              objectType, null, lock, true));
        }
      }
      return list;
    } catch (Exception e) {
      throw new KettleException("Unable to get list of objects from directory [" + dirId + "]", e);
    }
  }

  protected RepositoryObjectType getObjectType(final String filename) throws KettleException {
    if (filename.endsWith(RepositoryObjectType.TRANSFORMATION.getExtension())) {
      return RepositoryObjectType.TRANSFORMATION;
    } else if (filename.endsWith(RepositoryObjectType.JOB.getExtension())) {
      return RepositoryObjectType.JOB;
    } else if (filename.endsWith(RepositoryObjectType.DATABASE.getExtension())) {
      return RepositoryObjectType.DATABASE;
    } else if (filename.endsWith(RepositoryObjectType.SLAVE_SERVER.getExtension())) {
      return RepositoryObjectType.SLAVE_SERVER;
    } else if (filename.endsWith(RepositoryObjectType.CLUSTER_SCHEMA.getExtension())) {
      return RepositoryObjectType.CLUSTER_SCHEMA;
    } else if (filename.endsWith(RepositoryObjectType.PARTITION_SCHEMA.getExtension())) {
      return RepositoryObjectType.PARTITION_SCHEMA;
    } else {
      throw new KettleException("Unable to get object type");
    }
  }

  public IUser getUserInfo() {
    return user;
  }

  public String getVersion() {
    return REPOSITORY_VERSION;
  }

  public void insertJobEntryDatabase(ObjectId idJob, ObjectId idJobentry, ObjectId idDatabase) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public ObjectId insertLogEntry(String description) throws KettleException {
    // We are not presently logging
    return null;
  }

  public void insertStepDatabase(ObjectId idTransformation, ObjectId idStep, ObjectId idDatabase)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public ClusterSchema loadClusterSchema(ObjectId idClusterSchema, List<SlaveServer> slaveServers, String versionId)
      throws KettleException {
    try {
      // We dont need to use slaveServer variable as the dataNoteToElement method finds the server from the repository
      NodeRepositoryFileData data = pur.getDataAtVersionForRead(idClusterSchema.getId(), versionId,
          NodeRepositoryFileData.class);
      RepositoryFile file = null;
      if (versionId != null) {
        file = pur.getFileAtVersion(idClusterSchema.getId(), versionId);
      } else {
        file = pur.getFileById(idClusterSchema.getId());  
      }
      return clusterTransformer.assemble(file, data, pur.getVersionSummary(idClusterSchema.getId(), versionId));
    } catch (Exception e) {
      throw new KettleException("Unable to load cluster schema with id [" + idClusterSchema + "]", e);
    }
  }

  public Condition loadConditionFromStepAttribute(ObjectId idStep, String code) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public DatabaseMeta loadDatabaseMetaFromJobEntryAttribute(ObjectId idJobentry, String nameCode, String idCode,
      List<DatabaseMeta> databases) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public DatabaseMeta loadDatabaseMetaFromStepAttribute(ObjectId idStep, String code, List<DatabaseMeta> databases)
      throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public PartitionSchema loadPartitionSchema(ObjectId partitionSchemaId, String versionId) throws KettleException {
    try {
      NodeRepositoryFileData data = pur.getDataAtVersionForRead(partitionSchemaId.getId(), versionId,
          NodeRepositoryFileData.class);
      RepositoryFile file = null;
      if (versionId != null) {
        file = pur.getFileAtVersion(partitionSchemaId.getId(), versionId);
      } else {
        file = pur.getFileById(partitionSchemaId.getId());  
      }
      return partitionSchemaTransformer.assemble(file, data, pur.getVersionSummary(partitionSchemaId.getId(), versionId));
    } catch (Exception e) {
      throw new KettleException("Unable to load partition schema with id [" + partitionSchemaId + "]", e);
    }
  }

  public SlaveServer loadSlaveServer(ObjectId idSlaveServer, String versionId) throws KettleException {
    try {
      NodeRepositoryFileData data = pur.getDataAtVersionForRead(idSlaveServer.getId(), versionId,
          NodeRepositoryFileData.class);
      RepositoryFile file = null;
      if (versionId != null) {
        file = pur.getFileAtVersion(idSlaveServer.getId(), versionId);
      } else {
        file = pur.getFileById(idSlaveServer.getId());  
      }
      return slaveTransformer.assemble(file, data, pur.getVersionSummary(idSlaveServer.getId(), versionId));
    } catch (Exception e) {
      throw new KettleException("Unable to load slave server with id [" + idSlaveServer + "]", e);
    }
  }

  public RepositoryLock lockJob(final ObjectId idJob, final String message) throws KettleException {
    lockFileById(idJob, message);
    return getLockById(idJob);
  }

  public RepositoryLock lockTransformation(final ObjectId idTransformation, final String message) throws KettleException {
    lockFileById(idTransformation, message);
    return getLockById(idTransformation);
  }

  protected void lockFileById(final ObjectId id, final String message) throws KettleException {
    pur.lockFile(id.getId(), message);
  }

  public boolean canUnlockFileById(final ObjectId id) {
    return pur.canUnlockFile(id.getId());
  }

  protected Map<RepositoryObjectType, List<? extends SharedObjectInterface>> loadAndCacheSharedObjects(final boolean deepCopy) throws KettleException {
    if (sharedObjectsByType == null) {
      try {
        sharedObjectsByType = new HashMap<RepositoryObjectType, List<? extends SharedObjectInterface>>();
        // Slave Servers are referenced by Cluster Schemas so they must be loaded first 
        readSharedObjects(sharedObjectsByType, RepositoryObjectType.DATABASE, RepositoryObjectType.PARTITION_SCHEMA,
                RepositoryObjectType.SLAVE_SERVER, RepositoryObjectType.CLUSTER_SCHEMA);
      } catch (Exception e) {
        sharedObjectsByType = null;
        // TODO i18n
        throw new KettleException("Unable to read shared objects from repository", e); //$NON-NLS-1$
      }
    }
    return deepCopy ? deepCopy(sharedObjectsByType) : sharedObjectsByType;
  }
  
  protected Map<RepositoryObjectType, List<? extends SharedObjectInterface>> loadAndCacheSharedObjects() throws KettleException {
    return loadAndCacheSharedObjects(true);
  }
  
  private Map<RepositoryObjectType, List<? extends SharedObjectInterface>> deepCopy(Map<RepositoryObjectType, List<? extends SharedObjectInterface>> orig) throws KettleException {
    Map<RepositoryObjectType, List<? extends SharedObjectInterface>> copy = new HashMap<RepositoryObjectType, List<? extends SharedObjectInterface>>();
    for (RepositoryObjectType type : orig.keySet()) {
      List<? extends SharedObjectInterface> value = orig.get(type);
      List<SharedObjectInterface> newValue = new ArrayList<SharedObjectInterface>(value.size());
      for (SharedObjectInterface obj : value) {
        SharedObjectInterface newValueItem = null;
        if(obj instanceof DatabaseMeta) {
          DatabaseMeta databaseMeta = (DatabaseMeta) ((DatabaseMeta) obj).clone();
          databaseMeta.setObjectId(((DatabaseMeta) obj).getObjectId());
          databaseMeta.clearChanged();
          newValueItem = databaseMeta;
        } else if (obj instanceof SlaveServer) {
          SlaveServer slaveServer = (SlaveServer) ((SlaveServer) obj).clone();
          slaveServer.setObjectId(((SlaveServer) obj).getObjectId());
          slaveServer.clearChanged();
          newValueItem = slaveServer;
        } else if (obj instanceof PartitionSchema) {
          PartitionSchema partitionSchema = (PartitionSchema) ((PartitionSchema) obj).clone();
          partitionSchema.setObjectId(((PartitionSchema) obj).getObjectId());
          partitionSchema.clearChanged();
          newValueItem = partitionSchema;
        } else if (obj instanceof ClusterSchema) {
          ClusterSchema clusterSchema = (ClusterSchema) ((ClusterSchema) obj).clone();
          clusterSchema.setObjectId(((ClusterSchema) obj).getObjectId());
          clusterSchema.clearChanged();
          newValueItem = clusterSchema;
        } else {
          throw new KettleException("unknown shared object class");
        }
        newValue.add(newValueItem);
      }
      copy.put(type, newValue);
    }
    return copy;
  }
  
  public SharedObjects readJobMetaSharedObjects(final JobMeta jobMeta) throws KettleException {
    return jobDelegate.loadSharedObjects(jobMeta, loadAndCacheSharedObjects());
  }
  
  public SharedObjects readTransSharedObjects(final TransMeta transMeta) throws KettleException {
    return transDelegate.loadSharedObjects(transMeta, loadAndCacheSharedObjects());
  }

  public ObjectId renameJob(final ObjectId idJob, final RepositoryDirectoryInterface newDirectory, final String newName)
      throws KettleException {
    if (newName != null) {
      // set new title
      RepositoryFile file = pur.getFileById(idJob.getId());
      file = new RepositoryFile.Builder(file).title(RepositoryFile.ROOT_LOCALE, newName).build();
      NodeRepositoryFileData data = pur
      .getDataAtVersionForRead(file.getId(), null, NodeRepositoryFileData.class);
      file = pur.updateFile(file, data, null);
    }
    pur.moveFile(idJob.getId(), calcDestAbsPath(idJob, newDirectory, newName, RepositoryObjectType.JOB), null);
    rootRef = null;
    return idJob;
  }

  public ObjectId renameTransformation(final ObjectId idTransformation, final RepositoryDirectoryInterface newDirectory,
      final String newName) throws KettleException {
    if (newName != null) {
      // set new title
      RepositoryFile file = pur.getFileById(idTransformation.getId());
      file = new RepositoryFile.Builder(file).title(RepositoryFile.ROOT_LOCALE, newName).build();
      NodeRepositoryFileData data = pur
      .getDataAtVersionForRead(file.getId(), null, NodeRepositoryFileData.class);
      file = pur.updateFile(file, data, null);
    }
    pur.moveFile(idTransformation.getId(), calcDestAbsPath(idTransformation, newDirectory, newName,
        RepositoryObjectType.TRANSFORMATION), null);   
    rootRef = null;
    return idTransformation;
  }

  protected String getParentPath(final String path) {
    if (path == null) {
      throw new IllegalArgumentException();
    } else if (RepositoryFile.SEPARATOR.equals(path)) {
      return null;
    }
    int lastSlashIndex = path.lastIndexOf(RepositoryFile.SEPARATOR);
    if (lastSlashIndex == 0) {
      return RepositoryFile.SEPARATOR;
    } else if (lastSlashIndex > 0) {
      return path.substring(0, lastSlashIndex);
    } else {
      throw new IllegalArgumentException();
    }
  }

  protected String calcDestAbsPath(final ObjectId id, final RepositoryDirectoryInterface newDirectory, final String newName,
      final RepositoryObjectType objectType) {
    String newDirectoryPath = getPath(null, newDirectory, null);
    RepositoryFile file = pur.getFileById(id.getId());
    StringBuilder buf = new StringBuilder(file.getPath().length());
    if (newDirectory != null) {
      buf.append(newDirectoryPath);
    } else {
      buf.append(getParentPath(file.getPath()));
    }
    buf.append(RepositoryFile.SEPARATOR);
    if (newName != null) {
      buf.append(checkAndSanitize(newName));
      if (!newName.endsWith(objectType.getExtension())) {
        buf.append(objectType.getExtension());
      }
    } else {
      buf.append(file.getName());
    }
    return buf.toString();
  }

  public void save(final RepositoryElementInterface element, final String versionComment,
      final ProgressMonitorListener monitor) throws KettleException {
    PentahoDscContent dscContent = PentahoLicenseVerifier.verify(new KParam(PurRepositoryMeta.BUNDLE_REF_NAME));
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
        case CLUSTER_SCHEMA:
          saveClusterSchema(element, versionComment);
          break;
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

  private boolean isRenamed(final RepositoryElementInterface element, final RepositoryFile file) throws KettleException {
    if (element.getObjectId() == null) {
      return false; // never been saved
    }
    String filename = element.getName();
    switch (element.getRepositoryElementType()) {
      case TRANSFORMATION:
        filename += RepositoryObjectType.TRANSFORMATION.getExtension();
        break;
      case JOB:
        filename += RepositoryObjectType.JOB.getExtension();
        break;
      case DATABASE:
        filename += RepositoryObjectType.DATABASE.getExtension();
        break;
      case SLAVE_SERVER:
        filename += RepositoryObjectType.SLAVE_SERVER.getExtension();
        break;
      case CLUSTER_SCHEMA:
        filename += RepositoryObjectType.CLUSTER_SCHEMA.getExtension();
        break;
      case PARTITION_SCHEMA:
        filename += RepositoryObjectType.PARTITION_SCHEMA.getExtension();
        break;
      default:
        throw new KettleException("unknown element type [" + element.getClass().getName()
            + "]");
    }
    if (!file.getName().equals(checkAndSanitize(filename))) {
      return true;
    }
    return false;
  }
  
  private void renameIfNecessary(final RepositoryElementInterface element, final RepositoryFile file) throws KettleException {
    if (!isRenamed(element, file)) {
      return;
    }
    
    ObjectId id = element.getObjectId();
    StringBuilder buf = new StringBuilder(file.getPath().length());
    buf.append(getParentPath(file.getPath()));
    buf.append(RepositoryFile.SEPARATOR);
    buf.append(checkAndSanitize(element.getName()));
    switch (element.getRepositoryElementType()) {
      case DATABASE:
        buf.append(RepositoryObjectType.DATABASE.getExtension());
        break;
      case SLAVE_SERVER:
        buf.append(RepositoryObjectType.SLAVE_SERVER.getExtension());
        break;
      case CLUSTER_SCHEMA:
        buf.append(RepositoryObjectType.CLUSTER_SCHEMA.getExtension());
        break;
      case PARTITION_SCHEMA:
        buf.append(RepositoryObjectType.PARTITION_SCHEMA.getExtension());
        break;
      default:
        throw new KettleException("It's not possible to rename Class [" + element.getClass().getName()
            + "] to the repository");
    }
    pur.moveFile(file.getId(), buf.toString(), null);
  }

  protected void saveJob0(final RepositoryElementInterface element, final String versionComment, final boolean saveSharedObjects, final boolean checkLock, final boolean checkRename, final boolean loadRevision) throws KettleException {
    if (saveSharedObjects) {
      jobDelegate.saveSharedObjects(element, versionComment);
    }
    boolean isUpdate = element.getObjectId() != null;
    RepositoryFile file = null;
    if (isUpdate) {
      ObjectId id = element.getObjectId();
      file = pur.getFileById(id.getId());
      if (checkLock && file.isLocked() && !canUnlockFileById(id)) {
        throw new KettleException("File is currently locked by another user for editing");
      }
      // update title and description
      file = new RepositoryFile.Builder(file)
        .title(RepositoryFile.ROOT_LOCALE, element.getName())
        .description(RepositoryFile.ROOT_LOCALE, Const.NVL(element.getDescription(), ""))
        .build();
      file = pur.updateFile(
          file, 
          new NodeRepositoryFileData(jobDelegate.elementToDataNode(element)), 
          versionComment
         );
      if (checkRename && isRenamed(element, file)) {
        renameJob(element.getObjectId(), null, element.getName());
      }
    } else {
      file = new RepositoryFile.Builder(checkAndSanitize(element.getName() + RepositoryObjectType.JOB.getExtension()))
          .versioned(true)
          .title(RepositoryFile.ROOT_LOCALE, element.getName())
          .description(RepositoryFile.ROOT_LOCALE, Const.NVL(element.getDescription(), ""))
          .build();
      file = pur.createFile(element.getRepositoryDirectory().getObjectId().getId(), 
          file,
          new NodeRepositoryFileData(jobDelegate.elementToDataNode(element)), 
          versionComment
         );
    }
    // side effects
    ObjectId objectId = new StringObjectId(file.getId().toString());
    element.setObjectId(objectId);
    if (loadRevision) {
      element.setObjectRevision(getObjectRevision(objectId, null));
    }
    if (element instanceof ChangedFlagInterface) {
      ((ChangedFlagInterface)element).clearChanged();
    }
  }
  
  protected void saveJob(final RepositoryElementInterface element, final String versionComment) throws KettleException {
    saveJob0(element, versionComment, true, true, true, true);
  }

  protected void saveTrans0(final RepositoryElementInterface element, final String versionComment, final boolean saveSharedObjects, final boolean checkLock, final boolean checkRename, final boolean loadRevision) throws KettleException {
    if (saveSharedObjects) {
      transDelegate.saveSharedObjects(element, versionComment);
    }
    boolean isUpdate = element.getObjectId() != null;
    RepositoryFile file = null;
    if (isUpdate) {
      ObjectId id = element.getObjectId();
      file = pur.getFileById(id.getId());
      if (checkLock && file.isLocked() && !canUnlockFileById(id)) {
        throw new KettleException("File is currently locked by another user for editing");
      }
      // update title and description
      file = new RepositoryFile.Builder(file)
        .title(RepositoryFile.ROOT_LOCALE, element.getName())
        .description(RepositoryFile.ROOT_LOCALE, Const.NVL(element.getDescription(), ""))
        .build();
      file = pur.updateFile(
          file, 
          new NodeRepositoryFileData(transDelegate.elementToDataNode(element)),
          versionComment
         );
      if (checkRename && isRenamed(element, file)) {
        renameTransformation(element.getObjectId(), null, element.getName());
      }
    } else {
      file = new RepositoryFile.Builder(checkAndSanitize(element.getName() + RepositoryObjectType.TRANSFORMATION.getExtension()))
          .versioned(true)
          .title(RepositoryFile.ROOT_LOCALE, element.getName())
          .description(RepositoryFile.ROOT_LOCALE, Const.NVL(element.getDescription(), "")).build();
      file = pur.createFile(element.getRepositoryDirectory().getObjectId().getId(),
                file,
                new NodeRepositoryFileData(transDelegate.elementToDataNode(element)), 
                versionComment
              );
    }
    // side effects
    ObjectId objectId = new StringObjectId(file.getId().toString());
    element.setObjectId(objectId);
    if (loadRevision) {
      element.setObjectRevision(getObjectRevision(objectId, null));
    }
    if (element instanceof ChangedFlagInterface) {
      ((ChangedFlagInterface)element).clearChanged();
    }

  }
  
  protected void saveTrans(final RepositoryElementInterface element, final String versionComment)
      throws KettleException {
    saveTrans0(element, versionComment, true, true, true, true);
  }

  protected void saveDatabaseMeta(final RepositoryElementInterface element, final String versionComment)
      throws KettleException {
    try {
    // Even if the object id is null, we still have to check if the element is not present in the PUR
    // For example, if we import data from an XML file and there is a element with the same name in it.
    //
    if (element.getObjectId() == null) {
      element.setObjectId(getDatabaseID(element.getName()));
    }

    boolean isUpdate = element.getObjectId() != null;
    RepositoryFile file = null;
    if (isUpdate) {
      file = pur.getFileById(element.getObjectId().getId());
      // update title
      file = new RepositoryFile.Builder(file).title(RepositoryFile.ROOT_LOCALE, element.getName()).build();
      file = pur.updateFile(file, new NodeRepositoryFileData(databaseMetaTransformer.elementToDataNode(element)),
          versionComment);
      renameIfNecessary(element, file);
    } else {
      file = new RepositoryFile.Builder(checkAndSanitize(element.getName() + RepositoryObjectType.DATABASE.getExtension())).title(RepositoryFile.ROOT_LOCALE, element.getName()).versioned(
          VERSION_SHARED_OBJECTS).build();
      file = pur.createFile(getDatabaseMetaParentFolderId(), file, new NodeRepositoryFileData(databaseMetaTransformer
          .elementToDataNode(element)), versionComment);
    }
    // side effects
    ObjectId objectId = new StringObjectId(file.getId().toString());
    element.setObjectId(objectId);
    element.setObjectRevision(getObjectRevision(objectId, null));
    if (element instanceof ChangedFlagInterface) {
      ((ChangedFlagInterface)element).clearChanged();
    }
    updateSharedObjectCache(element);
    } catch (KettleException ke) {
      ke.printStackTrace();
    }
    
  }

  public DatabaseMeta loadDatabaseMeta(final ObjectId databaseId, final String versionId) throws KettleException {
    try {
      NodeRepositoryFileData data = pur.getDataAtVersionForRead(databaseId.getId(), versionId,
          NodeRepositoryFileData.class);
      RepositoryFile file = null;
      if (versionId != null) {
        file = pur.getFileAtVersion(databaseId.getId(), versionId);
      } else {
        file = pur.getFileById(databaseId.getId());  
      }
      return databaseMetaTransformer.assemble(file, data, pur.getVersionSummary(databaseId.getId(), versionId));
    } catch (Exception e) {
      throw new KettleException("Unable to load database with id [" + databaseId + "]", e);
    }
  }

  public TransMeta loadTransformation(final String transName, final RepositoryDirectoryInterface parentDir,
      final ProgressMonitorListener monitor, final boolean setInternalVariables, final String versionId)
      throws KettleException {
    String absPath = null;
    try {
      absPath = getPath(transName, parentDir, RepositoryObjectType.TRANSFORMATION);
      PentahoDscContent dscContent = PentahoLicenseVerifier.verify(new KParam(PurRepositoryMeta.BUNDLE_REF_NAME));
      RepositoryFile file = pur.getFile(absPath);
      if (versionId != null) {
        // need to go back to server to get versioned info
        file = pur.getFileAtVersion(file.getId(), versionId);
      }
      NodeRepositoryFileData data = null;
      ObjectRevision revision = null;
      // Additional obfuscation through obscurity
      if (dscContent != null) {
        data = pur.getDataAtVersionForRead(file.getId(), versionId, NodeRepositoryFileData.class);
        revision = getObjectRevision(new StringObjectId(file.getId().toString()), versionId);
      }
      return buildTransMeta(file, parentDir, data, revision);
    } catch (Exception e) {
      throw new KettleException("Unable to load transformation from path [" + absPath + "]", e);
    }
  }

  private TransMeta buildTransMeta(final RepositoryFile file, final RepositoryDirectoryInterface parentDir,
      final NodeRepositoryFileData data, final ObjectRevision revision) throws KettleException {
    TransMeta transMeta = new TransMeta();
    transMeta.setName(file.getTitle());
    transMeta.setDescription(file.getDescription());
    transMeta.setObjectId(new StringObjectId(file.getId().toString()));
    transMeta.setObjectRevision(revision);
    transMeta.setRepositoryDirectory(parentDir);
    readTransSharedObjects(transMeta); // This should read from the local cache
    transDelegate.dataNodeToElement(data.getNode(), transMeta);
    transMeta.clearChanged();
    return transMeta;
  }

  /**
   * Load all transformations referenced by {@code files}.
   * 
   * @param monitor
   * @param log
   * @param files Transformation files to load.
   * @param setInternalVariables Should internal variables be set when loading?  (Note: THIS IS IGNORED, they are always set)
   * @return Loaded transformations
   * 
   * @throws KettleException Error loading data for transformations from repository
   */
  protected List<TransMeta> loadTransformations(final ProgressMonitorListener monitor, final LogChannelInterface log,
      final List<RepositoryFile> files, final boolean setInternalVariables) throws KettleException {
    List<TransMeta> transformations = new ArrayList<TransMeta>(files.size());
    List<NodeRepositoryFileData> filesData = pur.getDataForReadInBatch(files, NodeRepositoryFileData.class);
    List<VersionSummary> versions = pur.getVersionSummaryInBatch(files);
    Iterator<RepositoryFile> filesIter = files.iterator();
    Iterator<NodeRepositoryFileData> filesDataIter = filesData.iterator();
    Iterator<VersionSummary> versionsIter = versions.iterator();
    while ((monitor == null || !monitor.isCanceled()) && filesIter.hasNext()) {
      RepositoryFile file = filesIter.next();
      NodeRepositoryFileData fileData = filesDataIter.next();
      VersionSummary version = versionsIter.next();
      String dirPath = file.getPath().substring(0, file.getPath().lastIndexOf(RepositoryDirectory.DIRECTORY_SEPARATOR));
      try {
        log.logDetailed("Loading/Exporting transformation [{0} : {1}]  ({2})", dirPath, file.getTitle(), file.getPath()); //$NON-NLS-1$
        if (monitor!=null) {
          monitor.subTask("Exporting transformation [" + file.getPath() + "]"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        transformations
            .add(buildTransMeta(file, findDirectory(dirPath), fileData, createObjectRevision(version)));
      } catch (Exception ex) {
        log.logDetailed("Unable to load transformation [" + file.getPath() + "]", ex); //$NON-NLS-1$ //$NON-NLS-2$
        log.logError("An error occurred reading transformation [" + file.getTitle() + "] from directory ["+dirPath+"] : " + ex.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        log.logError("Transformation [" + file.getTitle() + "] from directory [" + dirPath + "] was not exported because of a loading error!"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
      }
    }
    return transformations;
  }
  
  public JobMeta loadJob(String jobname, RepositoryDirectoryInterface parentDir, ProgressMonitorListener monitor,
      String versionId) throws KettleException {
    String absPath = null;
    try {
      absPath = getPath(jobname, parentDir, RepositoryObjectType.JOB);
      RepositoryFile file = pur.getFile(absPath);
      if (versionId != null) {
        // need to go back to server to get versioned info
        file = pur.getFileAtVersion(file.getId(), versionId);
      }
      PentahoDscContent dscContent = PentahoLicenseVerifier.verify(new KParam(PurRepositoryMeta.BUNDLE_REF_NAME));
      NodeRepositoryFileData data = null;
      ObjectRevision revision = null;
      // Additional obfuscation through obscurity
      if (dscContent != null) {
        data = pur.getDataAtVersionForRead(file.getId(), versionId, NodeRepositoryFileData.class);
        revision = getObjectRevision(new StringObjectId(file.getId().toString()), versionId);
      }
      return buildJobMeta(file, parentDir, data, revision);
    } catch (Exception e) {
      throw new KettleException("Unable to load transformation from path [" + absPath + "]", e);
    }
  }

  private JobMeta buildJobMeta(final RepositoryFile file, final RepositoryDirectoryInterface parentDir,
      final NodeRepositoryFileData data, final ObjectRevision revision) throws KettleException {
    JobMeta jobMeta = new JobMeta();
    jobMeta.setName(file.getTitle());
    jobMeta.setDescription(file.getDescription());
    jobMeta.setObjectId(new StringObjectId(file.getId().toString()));
    jobMeta.setObjectRevision(revision);
    jobMeta.setRepositoryDirectory(parentDir);
    readJobMetaSharedObjects(jobMeta); // This should read from the local cache
    jobDelegate.dataNodeToElement(data.getNode(), jobMeta);
    jobMeta.clearChanged();
    return jobMeta;
  }
  
  /**
   * Load all jobs referenced by {@code files}.
   * 
   * @param monitor
   * @param log
   * @param files Job files to load.
   * @param setInternalVariables Should internal variables be set when loading?  (Note: THIS IS IGNORED, they are always set)
   * @return Loaded jobs
   * 
   * @throws KettleException Error loading data for jobs from repository
   */
  protected List<JobMeta> loadJobs(final ProgressMonitorListener monitor, final LogChannelInterface log,
      final List<RepositoryFile> files, final boolean setInternalVariables) throws KettleException {
    List<JobMeta> transformations = new ArrayList<JobMeta>(files.size());
    List<NodeRepositoryFileData> filesData = pur.getDataForReadInBatch(files, NodeRepositoryFileData.class);
    List<VersionSummary> versions = pur.getVersionSummaryInBatch(files);
    Iterator<RepositoryFile> filesIter = files.iterator();
    Iterator<NodeRepositoryFileData> filesDataIter = filesData.iterator();
    Iterator<VersionSummary> versionsIter = versions.iterator();
    while ((monitor == null || !monitor.isCanceled()) && filesIter.hasNext()) {
      RepositoryFile file = filesIter.next();
      NodeRepositoryFileData fileData = filesDataIter.next();
      VersionSummary version = versionsIter.next();
      try {
        String dirPath = file.getPath().substring(0, file.getPath().lastIndexOf(RepositoryDirectory.DIRECTORY_SEPARATOR));
        log.logDetailed("Loading/Exporting job [{0} : {1}]  ({2})", dirPath, file.getTitle(), file.getPath()); //$NON-NLS-1$
        if (monitor!=null) {
          monitor.subTask("Exporting job [" + file.getPath() + "]"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        transformations
            .add(buildJobMeta(file, findDirectory(dirPath), fileData, createObjectRevision(version)));
      } catch (Exception ex) {
        log.logError("Unable to load job [" + file.getPath() + "]", ex); //$NON-NLS-1$ //$NON-NLS-2$
      }
    }
    return transformations;

  }
  
  /**
   * Performs one-way conversion on incoming String to produce a syntactically valid JCR path (section 4.6 Path Syntax). 
   */
  protected static String checkAndSanitize(final String in) {
    if (in == null) {
      throw new IllegalArgumentException();
    }
    String extension = null;
    if (in.endsWith(RepositoryObjectType.CLUSTER_SCHEMA.getExtension())) {
      extension = RepositoryObjectType.CLUSTER_SCHEMA.getExtension();
    } else if (in.endsWith(RepositoryObjectType.DATABASE.getExtension())) {
      extension = RepositoryObjectType.DATABASE.getExtension();
    } else if (in.endsWith(RepositoryObjectType.JOB.getExtension())) {
      extension = RepositoryObjectType.JOB.getExtension();
    } else if (in.endsWith(RepositoryObjectType.PARTITION_SCHEMA.getExtension())) {
      extension = RepositoryObjectType.PARTITION_SCHEMA.getExtension();
    } else if (in.endsWith(RepositoryObjectType.SLAVE_SERVER.getExtension())) {
      extension = RepositoryObjectType.SLAVE_SERVER.getExtension();
    } else if (in.endsWith(RepositoryObjectType.TRANSFORMATION.getExtension())) {
      extension = RepositoryObjectType.TRANSFORMATION.getExtension();
    }
    String out = in;
    if (extension != null) {
      out = out.substring(0, out.length()-extension.length());
    }
    if (out.contains("/") || out.equals("..") || out.equals(".") || StringUtils.isBlank(out)) {
      throw new IllegalArgumentException();
    }
    out = out.replaceAll("[/:\\[\\]\\*'\"\\|\\s\\.]", "_");  //$NON-NLS-1$//$NON-NLS-2$
    if (extension != null) {
      return out + extension;
    } else {
      return out;
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
    try {
      // Even if the object id is null, we still have to check if the element is not present in the PUR
      // For example, if we import data from an XML file and there is a element with the same name in it.
      //
      if (element.getObjectId() == null) {
        element.setObjectId(getPartitionSchemaID(element.getName()));
      }
      
      boolean isUpdate = element.getObjectId() != null;
      RepositoryFile file = null;
      if (isUpdate) {
        file = pur.getFileById(element.getObjectId().getId());
        // update title
        file = new RepositoryFile.Builder(file).title(RepositoryFile.ROOT_LOCALE, element.getName()).build();
        file = pur.updateFile(file, new NodeRepositoryFileData(partitionSchemaTransformer.elementToDataNode(element)),
            versionComment);
        renameIfNecessary(element, file);
      } else {
        file = new RepositoryFile.Builder(checkAndSanitize(element.getName() + RepositoryObjectType.PARTITION_SCHEMA.getExtension()))
        .title(RepositoryFile.ROOT_LOCALE, element.getName()).versioned(VERSION_SHARED_OBJECTS).build();
        file = pur.createFile(getPartitionSchemaParentFolderId(), file, new NodeRepositoryFileData(
            partitionSchemaTransformer.elementToDataNode(element)), versionComment);
      }
      // side effects
      ObjectId objectId = new StringObjectId(file.getId().toString());
      element.setObjectId(objectId);
      element.setObjectRevision(getObjectRevision(objectId, null));
      if (element instanceof ChangedFlagInterface) {
        ((ChangedFlagInterface)element).clearChanged();
      }
      updateSharedObjectCache(element);
    } catch (KettleException ke) {
      ke.printStackTrace();
    }
  }

  protected void saveSlaveServer(final RepositoryElementInterface element, final String versionComment) throws KettleException {
    try {
      // Even if the object id is null, we still have to check if the element is not present in the PUR
      // For example, if we import data from an XML file and there is a element with the same name in it.
      //
      if (element.getObjectId() == null) {
        element.setObjectId(getSlaveID(element.getName()));
      }
  
      boolean isUpdate = element.getObjectId() != null;
      RepositoryFile file = null;

      if (isUpdate) {
        file = pur.getFileById(element.getObjectId().getId());
        // update title
        file = new RepositoryFile.Builder(file).title(RepositoryFile.ROOT_LOCALE, element.getName()).build();
        file = pur.updateFile(file, new NodeRepositoryFileData(slaveTransformer.elementToDataNode(element)),
            versionComment);
        renameIfNecessary(element, file);
      } else {
        file = new RepositoryFile.Builder(checkAndSanitize(element.getName() + RepositoryObjectType.SLAVE_SERVER.getExtension()))
        .title(RepositoryFile.ROOT_LOCALE, element.getName()).versioned(VERSION_SHARED_OBJECTS).build();
        file = pur.createFile(getSlaveServerParentFolderId(), file, new NodeRepositoryFileData(slaveTransformer
            .elementToDataNode(element)), versionComment);
      }
      // side effects
      ObjectId objectId = new StringObjectId(file.getId().toString());
      element.setObjectId(objectId);
      element.setObjectRevision(getObjectRevision(objectId, null));
      if (element instanceof ChangedFlagInterface) {
        ((ChangedFlagInterface)element).clearChanged();
      }
      updateSharedObjectCache(element);
    } catch (KettleException ke) {
      ke.printStackTrace();
    }

  }

  protected void saveClusterSchema(final RepositoryElementInterface element, final String versionComment) {
    try {
      // Even if the object id is null, we still have to check if the element is not present in the PUR
      // For example, if we import data from an XML file and there is a element with the same name in it.
      //
      if (element.getObjectId() == null) {
        element.setObjectId(getClusterID(element.getName()));
      }
      
      boolean isUpdate = element.getObjectId() != null;
      RepositoryFile file = null;
      if (isUpdate) {
        file = pur.getFileById(element.getObjectId().getId());
        // update title
        file = new RepositoryFile.Builder(file)
          .title(RepositoryFile.ROOT_LOCALE, element.getName())
          .description(RepositoryFile.ROOT_LOCALE, Const.NVL(element.getDescription(), ""))
          .build();
        file = pur.updateFile(
            file, 
            new NodeRepositoryFileData(clusterTransformer.elementToDataNode(element)),
            versionComment
           );
        renameIfNecessary(element, file);
      } else {
        file = new RepositoryFile.Builder(checkAndSanitize(element.getName() + RepositoryObjectType.CLUSTER_SCHEMA.getExtension()))
          .title(RepositoryFile.ROOT_LOCALE, element.getName())
          .description(RepositoryFile.ROOT_LOCALE, Const.NVL(element.getDescription(), ""))
          .versioned(VERSION_SHARED_OBJECTS)
          .build();
        file = pur.createFile(
            getClusterSchemaParentFolderId(), 
            file, 
            new NodeRepositoryFileData(clusterTransformer.elementToDataNode(element)), 
            versionComment
           );
      }
      // side effects
      ObjectId objectId = new StringObjectId(file.getId().toString());
      element.setObjectId(objectId);
      element.setObjectRevision(getObjectRevision(objectId, null));
      if (element instanceof ChangedFlagInterface) {
        ((ChangedFlagInterface)element).clearChanged();
      }
      
      updateSharedObjectCache(element);
    } catch (KettleException ke) {
      ke.printStackTrace();
    }

  }

  private void updateSharedObjectCache(final RepositoryElementInterface element) throws KettleException {
    updateSharedObjectCache(element, null, null);
  }
  
  private void removeFromSharedObjectCache(final RepositoryObjectType type, final ObjectId id) throws KettleException {
    updateSharedObjectCache(null, type, id);
  }
  
  /**
   * Do not call this method directly.  Instead call updateSharedObjectCache or removeFromSharedObjectCache.
   */
  private void updateSharedObjectCache(final RepositoryElementInterface element, final RepositoryObjectType type, final ObjectId id) throws KettleException {
    if (element != null && (element.getObjectId() == null || element.getObjectId().getId() == null)) {
      throw new IllegalArgumentException(element.getName() + " has a null id");
    }
    
    loadAndCacheSharedObjects(false);
    
    boolean remove = element == null;
    ObjectId idToFind = element != null ? element.getObjectId() : id;
    RepositoryObjectType typeToUpdate = element != null ? element.getRepositoryElementType() : type;
    RepositoryElementInterface elementToUpdate = null;
    List<? extends SharedObjectInterface> origSharedObjects = null;
    switch (typeToUpdate) {
      case DATABASE:
        origSharedObjects = sharedObjectsByType.get(RepositoryObjectType.DATABASE);
        if (!remove) {
          elementToUpdate = (RepositoryElementInterface) ((DatabaseMeta) element).clone();
        }
        break;
      case SLAVE_SERVER:
        origSharedObjects = sharedObjectsByType.get(RepositoryObjectType.SLAVE_SERVER);
        if (!remove) {
          elementToUpdate = (RepositoryElementInterface) ((SlaveServer) element).clone();
        }
        break;
      case CLUSTER_SCHEMA:
        origSharedObjects = sharedObjectsByType.get(RepositoryObjectType.CLUSTER_SCHEMA);
        if (!remove) {
          elementToUpdate = (RepositoryElementInterface) ((ClusterSchema) element).clone();
        }
        break;
      case PARTITION_SCHEMA:
        origSharedObjects = sharedObjectsByType.get(RepositoryObjectType.PARTITION_SCHEMA);
        if (!remove) {
          elementToUpdate = (RepositoryElementInterface) ((PartitionSchema) element).clone();
        }
        break;
      default:
        throw new KettleException("unknown type [" + typeToUpdate + "]");
    }
    List<SharedObjectInterface> newSharedObjects = new ArrayList<SharedObjectInterface>(origSharedObjects);    
    // if there's a match on id, replace the element
    boolean found = false;
    for (int i = 0; i < origSharedObjects.size(); i++) {
      if (((RepositoryElementInterface) origSharedObjects.get(i)).getObjectId().equals(idToFind)) {
        if (remove) {
          newSharedObjects.remove(i);
        } else {
          elementToUpdate.setObjectId(idToFind); // because some clones don't clone the ID!!!
          newSharedObjects.set(i, (SharedObjectInterface) elementToUpdate);
        }
        found = true;
      }
    }
    // otherwise, add it
    if (!remove && !found) {
      elementToUpdate.setObjectId(idToFind); // because some clones don't clone the ID!!!
      newSharedObjects.add((SharedObjectInterface) elementToUpdate);
    }
    sharedObjectsByType.put(typeToUpdate, newSharedObjects);
  }

  private ObjectRevision getObjectRevision(final ObjectId elementId, final String versionId) {
    return createObjectRevision(pur.getVersionSummary(elementId.getId(), versionId));
  }

  /**
   * @return Wrapped {@link VersionSummary} with a {@link ObjectRevision}.
   */
  protected ObjectRevision createObjectRevision(final VersionSummary versionSummary) {
    return new PurObjectRevision(versionSummary.getId(), versionSummary.getAuthor(), versionSummary.getDate(),
        versionSummary.getMessage());
  }

  private String getDatabaseMetaParentFolderPath() {
    return ClientRepositoryPaths.getEtcFolderPath() + RepositoryFile.SEPARATOR + FOLDER_PDI + RepositoryFile.SEPARATOR
        + FOLDER_DATABASES;
  }

  private Serializable getDatabaseMetaParentFolderId() {
    if (cachedDatabaseMetaParentFolderId == null) {
      RepositoryFile f = pur.getFile(getDatabaseMetaParentFolderPath());
      cachedDatabaseMetaParentFolderId = f.getId();
    }
    return cachedDatabaseMetaParentFolderId;
  }

  private String getPartitionSchemaParentFolderPath() {
    return ClientRepositoryPaths.getEtcFolderPath() + RepositoryFile.SEPARATOR + FOLDER_PDI + RepositoryFile.SEPARATOR
        + FOLDER_PARTITION_SCHEMAS;
  }

  private Serializable getPartitionSchemaParentFolderId() {
    if (cachedPartitionSchemaParentFolderId == null) {
      RepositoryFile f = pur.getFile(getPartitionSchemaParentFolderPath());
      cachedPartitionSchemaParentFolderId = f.getId();
    }
    return cachedPartitionSchemaParentFolderId;
  }

  private String getSlaveServerParentFolderPath() {
    return ClientRepositoryPaths.getEtcFolderPath() + RepositoryFile.SEPARATOR + FOLDER_PDI + RepositoryFile.SEPARATOR
        + FOLDER_SLAVE_SERVERS;
  }

  private Serializable getSlaveServerParentFolderId() {
    if (cachedSlaveServerParentFolderId == null) {
      RepositoryFile f = pur.getFile(getSlaveServerParentFolderPath());
      cachedSlaveServerParentFolderId = f.getId();
    }
    return cachedSlaveServerParentFolderId;
  }

  private String getClusterSchemaParentFolderPath() {
    return ClientRepositoryPaths.getEtcFolderPath() + RepositoryFile.SEPARATOR + FOLDER_PDI + RepositoryFile.SEPARATOR
        + FOLDER_CLUSTER_SCHEMAS;
  }

  private Serializable getClusterSchemaParentFolderId() {
    if (cachedClusterSchemaParentFolderId == null) {
      RepositoryFile f = pur.getFile(getClusterSchemaParentFolderPath());
      cachedClusterSchemaParentFolderId = f.getId();
    }
    return cachedClusterSchemaParentFolderId;
  }

  public void saveConditionStepAttribute(ObjectId idTransformation, ObjectId idStep, String code, Condition condition)
      throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public void saveDatabaseMetaJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, String nameCode, String idCode,
      DatabaseMeta database) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public void saveDatabaseMetaStepAttribute(ObjectId idTransformation, ObjectId idStep, String code,
      DatabaseMeta database) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public void saveJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, String code, String value)
      throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public void saveJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, String code, boolean value)
      throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public void saveJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, String code, long value)
      throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public void saveJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, int nr, String code, String value)
      throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public void saveJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, int nr, String code, boolean value)
      throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public void saveJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, int nr, String code, long value)
      throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, String code, String value)
      throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, String code, boolean value)
      throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, String code, long value)
      throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, String code, double value)
      throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, int nr, String code, String value)
      throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, int nr, String code, boolean value)
      throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, int nr, String code, long value)
      throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, int nr, String code, double value)
      throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public void undeleteObject(final RepositoryElementMetaInterface element) throws KettleException {

    RepositoryFile originalParentFolder = pur.getFile(getPath(null, element.getRepositoryDirectory(), null));
    List<RepositoryFile> deletedChildren = pur.getDeletedFiles(originalParentFolder.getId(), checkAndSanitize(element.getName()
        + element.getObjectType().getExtension()));
    if (!deletedChildren.isEmpty()) {
      pur.undeleteFile(deletedChildren.get(0).getId(), null);
    }
    rootRef = null;
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

  public ObjectAcl getAcl(ObjectId fileId, boolean forceParentInheriting) throws KettleException {
    RepositoryFileAcl acl = null;
    try {
      acl = pur.getAcl(fileId.getId());
    } catch (Exception drfe) {
      // The user does not have rights to view the acl information. 
      throw new KettleException(drfe);
    }
    RepositoryFileSid sid = acl.getOwner();
    ObjectRecipient owner = new RepositoryObjectRecipient(sid.getName());
    if (sid.getType().equals(RepositoryFileSid.Type.USER)) {
      owner.setType(Type.USER);
    } else {
      owner.setType(Type.ROLE);
    }

    ObjectAcl objectAcl = new RepositoryObjectAcl(owner);
    List<RepositoryFileAce> aces;

    // This flag (forceParentInheriting) is here to allow us to query the acl AS IF 'inherit from parent'
    // were true, without committing the flag to the repository. We need this for state representation 
    // while a user is changing the acl in the client dialogs.

    if (forceParentInheriting) {
      objectAcl.setEntriesInheriting(true);
      aces = pur.getEffectiveAces(acl.getId(), true);
    } else {
      objectAcl.setEntriesInheriting(acl.isEntriesInheriting());
      aces = (acl.isEntriesInheriting()) ? pur.getEffectiveAces(acl.getId()) : acl.getAces();
    }
    List<ObjectAce> objectAces = new ArrayList<ObjectAce>();
    for (RepositoryFileAce ace : aces) {
      EnumSet<RepositoryFilePermission> permissions = ace.getPermissions();
      EnumSet<ObjectPermission> permissionSet = EnumSet.noneOf(ObjectPermission.class);
      RepositoryFileSid aceSid = ace.getSid();
      ObjectRecipient recipient = new RepositoryObjectRecipient(aceSid.getName());
      if (aceSid.getType().equals(RepositoryFileSid.Type.USER)) {
        recipient.setType(Type.USER);
      } else {
        recipient.setType(Type.ROLE);
      }
      for (RepositoryFilePermission perm : permissions) {
        if (perm.equals(RepositoryFilePermission.READ)) {
          permissionSet.add(ObjectPermission.READ);
        } else if (perm.equals(RepositoryFilePermission.READ_ACL)) {
          permissionSet.add(ObjectPermission.READ_ACL);
        } else if (perm.equals(RepositoryFilePermission.WRITE)) {
          permissionSet.add(ObjectPermission.WRITE);
        } else if (perm.equals(RepositoryFilePermission.WRITE_ACL)) {
          permissionSet.add(ObjectPermission.WRITE_ACL);
        } else if (perm.equals(RepositoryFilePermission.ALL)) {
          permissionSet.add(ObjectPermission.ALL);
        }
      }

      objectAces.add(new RepositoryObjectAce(recipient, permissionSet));

    }
    objectAcl.setAces(objectAces);
    return objectAcl;
  }

  public List<ObjectRevision> getRevisions(ObjectId fileId) throws KettleException {
    String absPath = null;
    try {
      List<ObjectRevision> versions = new ArrayList<ObjectRevision>();
      List<VersionSummary> versionSummaries = pur.getVersionSummaries(fileId.getId());
      for (VersionSummary versionSummary : versionSummaries) {
        versions.add(new PurObjectRevision(versionSummary.getId(), versionSummary.getAuthor(),
            versionSummary.getDate(), versionSummary.getMessage()));
      }
      return versions;
    } catch (Exception e) {
      throw new KettleException("Could not retrieve version history of object with path [" + absPath + "]", e);
    }
  }

  public void setAcl(ObjectId fileId, ObjectAcl objectAcl) throws KettleException {
    try {
      RepositoryFileAcl acl = pur.getAcl(fileId.getId());
      RepositoryFileAcl newAcl = new RepositoryFileAcl.Builder(acl).entriesInheriting(objectAcl.isEntriesInheriting())
          .clearAces().build();
      List<ObjectAce> aces = objectAcl.getAces();
      for (ObjectAce objectAce : aces) {

        EnumSet<ObjectPermission> permissions = objectAce.getPermissions();
        EnumSet<RepositoryFilePermission> permissionSet = EnumSet.noneOf(RepositoryFilePermission.class);
        ObjectRecipient recipient = objectAce.getRecipient();
        RepositoryFileSid sid;
        if (recipient.getType().equals(Type.ROLE)) {
          sid = new RepositoryFileSid(recipient.getName(), RepositoryFileSid.Type.ROLE);
        } else {
          sid = new RepositoryFileSid(recipient.getName());
        }
        if (permissions != null) {
          for (ObjectPermission perm : permissions) {
            if (perm.equals(ObjectPermission.READ)) {
              permissionSet.add(RepositoryFilePermission.READ);
            } else if (perm.equals(ObjectPermission.READ_ACL)) {
              permissionSet.add(RepositoryFilePermission.READ_ACL);
            } else if (perm.equals(ObjectPermission.WRITE)) {
              permissionSet.add(RepositoryFilePermission.WRITE);
            } else if (perm.equals(ObjectPermission.WRITE_ACL)) {
              permissionSet.add(RepositoryFilePermission.WRITE_ACL);
            } else if (perm.equals(ObjectPermission.ALL)) {
              permissionSet.add(RepositoryFilePermission.ALL);
            }
          }
        }
        newAcl = new RepositoryFileAcl.Builder(newAcl).ace(sid, permissionSet).build();
      }
      pur.updateAcl(newAcl);
    } catch (Exception drfe) {
      // The user does not have rights to view or set the acl information. 
      throw new KettleException(drfe);
    }
  }

  public List<RepositoryElementMetaInterface> getJobAndTransformationObjects(ObjectId id_directory, boolean includeDeleted)
      throws KettleException {
    return getPdiObjects(id_directory, Arrays.asList(new RepositoryObjectType[] { RepositoryObjectType.JOB,
        RepositoryObjectType.TRANSFORMATION }), includeDeleted);
  }

  public IRepositoryService getService(Class<? extends IRepositoryService> clazz) throws KettleException {
    return serviceMap.get(clazz);
  }

  public List<Class<? extends IRepositoryService>> getServiceInterfaces() throws KettleException {
    return serviceList;
  }

  public boolean hasService(Class<? extends IRepositoryService> clazz) throws KettleException {
    return serviceMap.containsKey(clazz);
  }

  public void delete(final List<ObjectId> ids) throws KettleException {
    for (ObjectId id : ids) {
      pur.deleteFile(id.getId(), true, null);
    }
  }

  public void undelete(final List<ObjectId> ids) throws KettleException {
    for (ObjectId id : ids) {
      pur.undeleteFile(id.getId(), null);
    }
  }

  public List<RepositoryObjectInterface> getTrash() throws KettleException {
    List<RepositoryObjectInterface> trash = new ArrayList<RepositoryObjectInterface>();
    List<RepositoryFile> deletedChildren = pur.getDeletedFiles();
    for (final RepositoryFile file : deletedChildren) {
      final Serializable originalParentFolderId = file.getOriginalParentFolderId();
      final RepositoryDirectory origParentDir = new RepositoryDirectory() {
        public ObjectId getObjectId() {
          return new StringObjectId(originalParentFolderId.toString());
        }
      };
      if (file.isFolder()) {
        trash.add(new RepositoryDirectory() {
          public String getName() {
            return file.getName();
          }

          public ObjectId getObjectId() {
            return new StringObjectId(file.getId().toString());
          }

          public RepositoryDirectoryInterface getParent() {
            return origParentDir;
          }
        });
      } else {
        RepositoryObjectType objectType = getObjectType(file.getName());
        trash.add(new EERepositoryObject(new StringObjectId(file.getId().toString()), file.getTitle(), origParentDir, null, file.getDeletedDate(),
            objectType, null, null, true));
      }
    }
    return trash;
  }

  public RepositoryDirectoryInterface getDefaultSaveDirectory(RepositoryElementInterface repositoryElement)
      throws KettleException {
    return getUserHomeDirectory();
  }

  public RepositoryDirectoryInterface getUserHomeDirectory() throws KettleException {
    return findDirectory(ClientRepositoryPaths.getUserHomeFolderPath(user.getLogin()));
  }
  
  public RepositoryObject getObjectInformation(ObjectId objectId, RepositoryObjectType objectType)
      throws KettleException {
    try {
      RepositoryFile repositoryFile = pur.getFileById(objectId.getId());
      if (repositoryFile==null) {
        return null;
      }
      String parentPath = getParentPath(repositoryFile.getPath());
      String name = repositoryFile.getTitle();
      String description = repositoryFile.getDescription();
      Date modifiedDate = repositoryFile.getLastModifiedDate();
      String ownerName = repositoryFile.getOwner().getName();
      boolean deleted = repositoryFile.getOriginalParentFolderPath() != null;
      RepositoryDirectoryInterface directory = findDirectory(parentPath);
      return new RepositoryObject(objectId, name, directory, ownerName, modifiedDate, objectType, description, deleted);
    } catch(Exception e) {
      throw new KettleException("Unable to get object information for object with id="+objectId, e);
    }
  }

  public RepositoryDirectoryInterface findDirectory(String directory) throws KettleException {
    RepositoryDirectoryInterface repositoryDirectoryInterface = null;
    // check if we have a rootRef cached
    boolean usingRootDirCache = rootRef != null && rootRef.get() != null;
    repositoryDirectoryInterface = getRootDir().findDirectory(directory);  
    // if we are using a cached version of the repository interface, allow a reload if we do not find
    if (repositoryDirectoryInterface == null && usingRootDirCache) {
      repositoryDirectoryInterface = loadRepositoryDirectoryTree().findDirectory(directory);
    } 
    return repositoryDirectoryInterface;
  }

  public RepositoryDirectoryInterface findDirectory(ObjectId directory) throws KettleException {
    RepositoryDirectoryInterface repositoryDirectoryInterface = null;
    // check if we have a rootRef cached
    boolean usingRootDirCache = rootRef != null && rootRef.get() != null;
    repositoryDirectoryInterface = getRootDir().findDirectory(directory);  
    // if we are using a cached version of the repository interface, allow a reload if we do not find
    if (repositoryDirectoryInterface == null && usingRootDirCache) {
      repositoryDirectoryInterface = loadRepositoryDirectoryTree().findDirectory(directory);
    } 
    return repositoryDirectoryInterface;
  }
  
  public JobMeta loadJob(ObjectId idJob, String versionLabel) throws KettleException {
    try {
      PentahoDscContent dscContent = PentahoLicenseVerifier.verify(new KParam(PurRepositoryMeta.BUNDLE_REF_NAME));
      RepositoryFile file = null;
      if (versionLabel != null) {
        file = pur.getFileAtVersion(idJob.getId(), versionLabel);
      } else {
        file = pur.getFileById(idJob.getId());  
      }
      EEJobMeta jobMeta = new EEJobMeta();
      jobMeta.setName(file.getTitle());
      jobMeta.setDescription(file.getDescription());
      jobMeta.setObjectId(new StringObjectId(file.getId().toString()));
      jobMeta.setObjectRevision(getObjectRevision(new StringObjectId(file.getId().toString()), versionLabel));
      jobMeta.setRepositoryDirectory(findDirectory(getParentPath(file.getPath())));

      readJobMetaSharedObjects(jobMeta);
      // Additional obfuscation through obscurity
      if (dscContent != null) {
        jobMeta.setRepositoryLock(getLock(file));
      }
      jobDelegate.dataNodeToElement(pur.getDataAtVersionForRead(idJob.getId(), versionLabel,
          NodeRepositoryFileData.class).getNode(), jobMeta);
      jobMeta.clearChanged();
      return jobMeta;
    } catch (Exception e) {
      throw new KettleException("Unable to load job with id [" + idJob + "]", e);
    }
  }

  public TransMeta loadTransformation(ObjectId idTransformation, String versionLabel) throws KettleException {
    try {
      RepositoryFile file = null;
      if (versionLabel != null) {
        file = pur.getFileAtVersion(idTransformation.getId(), versionLabel);
      } else {
        file = pur.getFileById(idTransformation.getId());  
      }
      PentahoDscContent dscContent = PentahoLicenseVerifier.verify(new KParam(PurRepositoryMeta.BUNDLE_REF_NAME));
      EETransMeta transMeta = new EETransMeta();
      transMeta.setName(file.getTitle());
      transMeta.setDescription(file.getDescription());
      transMeta.setObjectId(new StringObjectId(file.getId().toString()));
      transMeta.setObjectRevision(getObjectRevision(new StringObjectId(file.getId().toString()), versionLabel));
      transMeta.setRepositoryDirectory(findDirectory(getParentPath(file.getPath())));
      transMeta.setRepositoryLock(getLock(file));
      
      readTransSharedObjects(transMeta);
      // Additional obfuscation through obscurity
      if (dscContent != null) {
        transDelegate.dataNodeToElement(pur.getDataAtVersionForRead(idTransformation.getId(), versionLabel,
            NodeRepositoryFileData.class).getNode(), transMeta);
      }
      transMeta.clearChanged();
      return transMeta;
    } catch (Exception e) {
      throw new KettleException("Unable to load transformation with id [" + idTransformation + "]", e);
    }
  }

  public String getConnectMessage() {
    return connectMessage;
  }

  public String[] getJobsUsingDatabase(ObjectId id_database) throws KettleException {
    List<String> result = new ArrayList<String>();
    for(RepositoryFile file : getReferrers(id_database, Arrays.asList(new RepositoryObjectType[]{RepositoryObjectType.JOB}))) {
        result.add(file.getPath());
    }
    return result.toArray(new String[result.size()]);
  }

  public String[] getTransformationsUsingDatabase(ObjectId id_database) throws KettleException {
    List<String> result = new ArrayList<String>();
    for(RepositoryFile file : getReferrers(id_database, Arrays.asList(new RepositoryObjectType[]{RepositoryObjectType.TRANSFORMATION}))) {
        result.add(file.getPath());
    }
    return result.toArray(new String[result.size()]);
  }
  
  protected List<RepositoryFile> getReferrers(ObjectId fileId) throws KettleException {
    return getReferrers(fileId, null);
  }
  
  protected List<RepositoryFile> getReferrers(ObjectId fileId, List<RepositoryObjectType> referrerTypes) throws KettleException {
    // Use a result list to append to; Removing from the files list was causing a concurrency exception
    List<RepositoryFile> result = new ArrayList<RepositoryFile>(); 
    List<RepositoryFile> files = pur.getReferrers(fileId.getId());
    
    // Filter out types
    if(referrerTypes != null && referrerTypes.size() > 0) {
      for(RepositoryFile file : files) {
        if(referrerTypes.contains(getObjectType(file.getName()))) {
          result.add(file);
        }
      }
    }
    
    return result;
  }
  
  public IRepositoryExporter getExporter() {
    return new PurRepositoryExporter(this);
  }
  
  public IRepositoryImporter getImporter() {
    return new PurRepositoryImporter(this);
  }
  
}