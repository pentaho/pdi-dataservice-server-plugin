package org.pentaho.di.repository.pur;

import java.io.Serializable;
import java.lang.ref.SoftReference;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.namespace.QName;
import javax.xml.ws.BindingProvider;
import javax.xml.ws.Service;

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
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.partition.PartitionSchema;
import org.pentaho.di.repository.AbsSecurityManager;
import org.pentaho.di.repository.AbsSecurityProvider;
import org.pentaho.di.repository.Directory;
import org.pentaho.di.repository.EEUserInfo;
import org.pentaho.di.repository.IAbsSecurityManager;
import org.pentaho.di.repository.IAbsSecurityProvider;
import org.pentaho.di.repository.IAclManager;
import org.pentaho.di.repository.IRepositoryService;
import org.pentaho.di.repository.IRoleSupportSecurityManager;
import org.pentaho.di.repository.ITrashService;
import org.pentaho.di.repository.IUser;
import org.pentaho.di.repository.ObjectAce;
import org.pentaho.di.repository.ObjectAcl;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.ObjectPermission;
import org.pentaho.di.repository.ObjectRecipient;
import org.pentaho.di.repository.ObjectRevision;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.RepositoryElement;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.repository.RepositoryElementLocationInterface;
import org.pentaho.di.repository.RepositoryLock;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.RepositoryObject;
import org.pentaho.di.repository.RepositoryObjectAce;
import org.pentaho.di.repository.RepositoryObjectAcl;
import org.pentaho.di.repository.RepositoryObjectRecipient;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.repository.RepositorySecurityManager;
import org.pentaho.di.repository.RepositorySecurityProvider;
import org.pentaho.di.repository.RepositoryVersionRegistry;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.repository.VersionRepository;
import org.pentaho.di.repository.ObjectRecipient.Type;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.repository.pur.services.RepositoryLockService;
import org.pentaho.di.ui.repository.repositoryexplorer.UISupportRegistery;
import org.pentaho.platform.api.repository.IUnifiedRepository;
import org.pentaho.platform.api.repository.RepositoryFile;
import org.pentaho.platform.api.repository.RepositoryFileAce;
import org.pentaho.platform.api.repository.RepositoryFileAcl;
import org.pentaho.platform.api.repository.RepositoryFilePermission;
import org.pentaho.platform.api.repository.RepositoryFileSid;
import org.pentaho.platform.api.repository.VersionSummary;
import org.pentaho.platform.engine.core.system.PentahoSessionHolder;
import org.pentaho.platform.engine.core.system.StandaloneSession;

import com.pentaho.commons.dsc.PentahoDscContent;
import com.pentaho.commons.dsc.PentahoLicenseVerifier;
import com.pentaho.commons.dsc.params.KParam;
import com.pentaho.repository.RepositoryPaths;
import com.pentaho.repository.pur.data.node.NodeRepositoryFileData;
import com.pentaho.repository.pur.ws.IUnifiedRepositoryWebService;
import com.pentaho.repository.pur.ws.UnifiedRepositoryToWebServiceAdapter;

/**
 * Implementation of {@link Repository} that delegates to the Pentaho unified repository (PUR), an instance of
 * {@link IUnifiedRepository}.
 * 
 * @author Matt
 * @author mlowery
 */
public class PurRepository implements Repository, VersionRepository, IAclManager, ITrashService
// , RevisionRepository 
{

  // ~ Static fields/initializers ======================================================================================

  private static final Log logger = LogFactory.getLog(PurRepository.class);

  private static final String REPOSITORY_VERSION = "1.0"; //$NON-NLS-1$

  private static final boolean VERSION_SHARED_OBJECTS = true;

  private static final String FOLDER_PDI = "pdi"; //$NON-NLS-1$

  private static final String FOLDER_PARTITION_SCHEMAS = "partitionSchemas"; //$NON-NLS-1$

  private static final String FOLDER_CLUSTER_SCHEMAS = "clusterSchemas"; //$NON-NLS-1$

  private static final String FOLDER_SLAVE_SERVERS = "slaveServers"; //$NON-NLS-1$

  private static final String FOLDER_DATABASES = "databases"; //$NON-NLS-1$

  // ~ Instance fields =================================================================================================

  private IUnifiedRepository pur;

  private IUser user;

  private PurRepositoryMeta repositoryMeta;

  private ITransformer databaseMetaTransformer = new DatabaseDelegate(this);

  private ITransformer partitionSchemaTransformer = new PartitionDelegate(this);

  private ITransformer slaveTransformer = new SlaveDelegate(this);

  private ITransformer clusterTransformer = new ClusterDelegate(this);

  private ISharedObjectsTransformer transDelegate = new TransDelegate(this);

  private ISharedObjectsTransformer jobDelegate = new JobDelegate(this);

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
  private SoftReference<RepositoryDirectory> rootRef = null;

  /** 
   * We need to cache the fact that the user home is aliased, in order to properly map back to a fully
   * qualified repository path for use in repository functions. This arises specifically because of the use case where a 
   * folder the same name as a user home folder can be created at the tenant root level; if that happens, then we 
   * don't alias the user's home directory, but we still have a folder at the tenant root level that LOOKS like and 
   * aliased user home folder. 
   */
  private boolean isUserHomeDirectoryAliased = false;

  protected Serializable userHomeAlias = null;

  private Map<Class<? extends IRepositoryService>, IRepositoryService> serviceMap;

  private List<Class<? extends IRepositoryService>> serviceList;

  // ~ Constructors ====================================================================================================

  public PurRepository() {
    super();
  }

  // ~ Methods =========================================================================================================

  protected RepositoryDirectory getRootDir() throws KettleException {
    if (rootRef != null && rootRef.get() != null) {
      return rootRef.get();
    } else {
      return loadRepositoryDirectoryTree();
    }
  }

  public void setPur(final IUnifiedRepository pur) {
    this.pur = pur;
  }

  public void init(final RepositoryMeta repositoryMeta) {
    this.log = new LogChannel(this);
    this.repositoryMeta = (PurRepositoryMeta) repositoryMeta;
    this.serviceMap = new HashMap<Class<? extends IRepositoryService>, IRepositoryService>();
    this.serviceList = new ArrayList<Class<? extends IRepositoryService>>();
  }

  public void connect(String username, String password) throws KettleException, KettleSecurityException {
    IUser user = new EEUserInfo();
    user.setLogin(username);
    user.setPassword(password);
    user.setName(username);
    this.user = user;
    // TODO: is this necessary in client side code? this could 
    //       cause problems when embedded in the platform.
    populatePentahoSessionHolder();

    try {
      final String url = repositoryMeta.getRepositoryLocation().getUrl() + "/unifiedRepository?wsdl";
      Service service = Service.create(new URL(url), new QName("http://www.pentaho.org/ws/1.0", "unifiedRepository"));

      IUnifiedRepositoryWebService repoWebService = service.getPort(IUnifiedRepositoryWebService.class);

      // http basic authentication
      ((BindingProvider) repoWebService).getRequestContext().put(BindingProvider.USERNAME_PROPERTY, username);
      ((BindingProvider) repoWebService).getRequestContext().put(BindingProvider.PASSWORD_PROPERTY, password);
      // accept cookies to maintain session on server
      ((BindingProvider) repoWebService).getRequestContext().put(BindingProvider.SESSION_MAINTAIN_PROPERTY, true);

      pur = new UnifiedRepositoryToWebServiceAdapter(repoWebService);
      userHomeAlias = pur.getFile(RepositoryPaths.getUserHomeFolderPath()).getId();
      // We need to add the service class in the list in the order of dependencies
      // IRoleSupportSecurityManager depends RepositorySecurityManager to be present
      securityProvider = new AbsSecurityProvider(this, this.repositoryMeta, user);
      registerRepositoryService(RepositorySecurityProvider.class, securityProvider);
      registerRepositoryService(IAbsSecurityProvider.class, securityProvider);
      
      registerRepositoryService(RepositoryLockService.class, new RepositoryLockService());

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
      registerRepositoryService(VersionRepository.class, this);
      registerRepositoryService(IAclManager.class, this);
      registerRepositoryService(ITrashService.class, this);
    } catch (Exception e) {
      throw new KettleException(e);
    }
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

  protected void populatePentahoSessionHolder() {
    // only necessary for RepositoryPaths calls; server also uses PentahoSessionHolder but that is in a different JVM
    PentahoSessionHolder.setSession(new StandaloneSession(user.getLogin()));
  }

  public boolean isConnected() {
    return PentahoSessionHolder.getSession() != null;
  }

  public void disconnect() {

  }

  public int countNrJobEntryAttributes(ObjectId idJobentry, String code) throws KettleException {
    // implemented by RepositoryProxy
    throw new UnsupportedOperationException();
  }

  public int countNrStepAttributes(ObjectId idStep, String code) throws KettleException {
    // implemented by RepositoryProxy
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
            .getObjectId().getId() : null, new RepositoryFile.Builder(dir.getName()).folder(true).build(), null);
        dir.setObjectId(new StringObjectId(newFolder.getId().toString()));
      }
    } catch (Exception e) {
      throw new KettleException("Unable to save repository directory with path [" + getPath(null, dir, null) + "]", e);
    }
  }

  public void deleteRepositoryDirectory(final RepositoryDirectory dir) throws KettleException {
    try {
      pur.deleteFile(dir.getObjectId().getId(), null);
    } catch (Exception e) {
      throw new KettleException("Unable to delete directory with path [" + getPath(null, dir, null) + "]", e);
    }
  }

  public ObjectId renameRepositoryDirectory(final RepositoryDirectory dir) throws KettleException {
    // dir ID is used to find orig obj; dir name is new name of obj; dir is not moved from its original loc
    try {
      String absPath = getPath(null, dir.getParent(), null);
      pur.moveFile(dir.getObjectId().getId(), absPath + RepositoryFile.SEPARATOR + dir.getName(), null);
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
    String interimFolderPath = null;
    try {
      RepositoryFile folder = pur.getFileById(dirId.getId());
      finalName = (newName != null ? newName : folder.getName());
      interimFolderPath = folder.getAbsolutePath().replace(RepositoryFile.SEPARATOR + folder.getName(), "");
      finalParentPath = (newParent != null ? getPath(null, newParent, null) : interimFolderPath);
      pur.moveFile(dirId.getId(), finalParentPath + RepositoryFile.SEPARATOR + finalName, null);
      return dirId;
    } catch (Exception e) {
      throw new KettleException("Unable to move/rename directory with id [" + dirId + "] to new parent ["
          + finalParentPath + "] and new name [nant" + finalName + "]", e);
    }
  }

  public RepositoryDirectory loadRepositoryDirectoryTree() throws KettleException {

    RepositoryFile pentahoRootFolder = pur.getFile(RepositoryPaths.getPentahoRootFolderPath());

    RepositoryDirectory root = new RepositoryDirectory();
    rootRef = new SoftReference<RepositoryDirectory>(root);

    root.setObjectId(null);
    RepositoryDirectory dir = new RepositoryDirectory(root, pentahoRootFolder.getName());
    dir.setObjectId(new StringObjectId(pentahoRootFolder.getId().toString()));
    root.addSubdirectory(dir);
    loadRepositoryDirectory(dir, pentahoRootFolder);

    /** HACK AND SLASH HERE ***/
    /**
    * This code accomodates the following parenting logic for display and navigation
    * of the Enterprise repository:
    * 
    * 1. Spoon's Repository Explorer should not be tenant aware. This means that the Repository Explorer will only show one tenant's content at a time. (Jake approved) 
    * 2. Management of multiple tenants shouldn't be dome in the design tools. A super admin tool is needed for multi-tenant administration. 
    * 3. Admin's perspective in Repository Explorer:
    *   a. The admin will see all folders in the tenant root folder as siblings, with an unnamed root. One of these folders will be the physical path from the tenant folder to the root of all users home folders. Under this node, the admin can see and access all users' home folders (dictated by ACLs, not business logic). 
      
    *   EXAMPLE: Admin logs in...
    *   ===================
    *   /
    *   /home/user2
    *   /home/user3
    *   /home/admin
    *   /public
    *   /extra1
    *   /extra2
    
    *   b. The admin will not see the protected physical multi-tenant structure (ie., the actual physical root of the repository to the tenant folder). This physical path to the tenant folder is also configurable and fetched from the server.
    
    *   EXAMPLE:
    *   ===================
    *   getRootFolder() returns "pentaho/acme", or whatever is in the configuration server-side. "pentaho/acme" is never shown in the UI, nor does the client code ever have to have knowledge of this structure. 
    
    * 4. User's perspective in Repository Explorer:
    *   a. The user should see a "home" folder, and a "public" folder. The home folder will appear as the user's login, aliased from it's actual physical path. The physical structure of the path to the home folder from the tenant folder will be fetched from the server by the client code, so as to be configurable. 
    
    *   EXAMPLE: Suzy logs in...
      ===================
    *   /
    *   /public
    *   /suzy (physically stored as /home/suzy)
      
    
    *   b. In the case where the admin has created a folder with the same name as the "home" folder alias, the "home" folder will appear as it's physical path and name.
    
    *   EXAMPLE: Admin logs in... and creates /suzy folder...
    *   ===================
    *   /
    *   /home/user2
    *   /home/suzy
    *   /home/admin
    *   /public
    *   /suzy
    *   /extra2
    
    *   ... then suzy logs in ...
    *   ===================
    *   /
    *   /public
    *   /suzy (the folder admin created)
    *   /home/suzy (suzy's home folder)

    * Pseudo-code:
    * ==============================================
    
    * Call 1: getTenant() ==> /tenant0
    * Call 2: get children down 2 levels
    * Call 3: getHomeFolder() ==> /home/me
    * BizLogic 1: chop off tenant node
    * BizLogic 2: if write() on home folder, skip;
    *   if sibling folder same name as alias ("me"), skip;
    *   alias /home/me to "me"  

    **********************************************************************************************/

    // Example: /pentaho/tenant0
    RepositoryDirectory tenantRoot = root.findDirectory(RepositoryPaths.getTenantRootFolderPath());
    // Example: /pentaho/tenant0/home
    RepositoryDirectory tenantHome = root.findDirectory(RepositoryPaths.getTenantHomeFolderPath());
    // Example: /pentaho/tenant0/home/suzy
    RepositoryDirectory userHome = root.findDirectory(RepositoryPaths.getUserHomeFolderPath());
    // Example: /pentaho/tenant0/etc
    RepositoryDirectory etcHome = root.findDirectory(RepositoryPaths.getTenantEtcFolderPath());
    String alias = userHome.getName();

    boolean hasHomeWriteAccess = pur.hasAccess(RepositoryPaths.getTenantHomeFolderPath(), EnumSet
        .of(RepositoryFilePermission.WRITE));

    // Skip aliasing the home directory if:
    // a. the user has write access to the home directory (signifying admin access)
    // b. an admin has inadvertently created a sibling folder with the same name as the alias we want to use. 

    isUserHomeDirectoryAliased = !(hasHomeWriteAccess || (tenantRoot.findChild(alias) != null));

    List<Directory> children = new ArrayList<Directory>();
    RepositoryDirectory newRoot = new RepositoryDirectory();
    newRoot.setObjectId(tenantRoot.getObjectId());

    for (int i = 0; i < tenantRoot.getNrSubdirectories(); i++) {
      RepositoryDirectory tenantChild = tenantRoot.getSubdirectory(i);
      boolean isEtcChild = tenantChild.equals(etcHome);
      if (isEtcChild) {
        continue;
      }
      boolean isHomeChild = tenantChild.equals(tenantHome);
      // We are now re-parenting to serve up the view that the UI would like to display...
      // We revert to the absolute paths need for repo functions in the getPath() method....
      if (isHomeChild && isUserHomeDirectoryAliased) {
        newRoot.addSubdirectory(userHome);
      } else {
        newRoot.addSubdirectory(tenantChild);
      }
    }
    /** END HACK AND SLASH HERE ***/
    return newRoot;
  }

  private void loadRepositoryDirectory(final RepositoryDirectory parentDir, final RepositoryFile folder)
      throws KettleException {
    try {
      List<RepositoryFile> children = pur.getChildren(folder.getId());
      for (RepositoryFile child : children) {
        if (child.isFolder()) {
          RepositoryDirectory dir = new RepositoryDirectory(parentDir, child.getName());
          dir.setObjectId(new StringObjectId(child.getId().toString()));
          parentDir.addSubdirectory(dir);
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
    try {
      pur.deleteFile(idCluster.getId(), null);
    } catch (Exception e) {
      throw new KettleException("Unable to delete cluster schema with name [" + idCluster + "]", e);
    }
  }

  public void deleteJob(ObjectId idJob) throws KettleException {
    deleteFileById(idJob);
  }

  public void deleteJob(ObjectId jobId, String versionId) throws KettleException {
    pur.deleteFileAtVersion(jobId.getId(), versionId);
  }

  public void deleteFileById(final ObjectId id) throws KettleException {
    try {
      pur.deleteFile(id.getId(), null);
    } catch (Exception e) {
      throw new KettleException("Unable to delete object with id [" + id + "]", e);
    }
  }

  public void restoreJob(ObjectId idJob, String versionId, String versionComment) {
    pur.restoreFileAtVersion(idJob.getId(), versionId, versionComment);
  }

  public void restoreTransformation(ObjectId idTransformation, String versionId, String versionComment)
      throws KettleException {
    pur.restoreFileAtVersion(idTransformation.getId(), versionId, versionComment);
  }

  public void deletePartitionSchema(ObjectId idPartitionSchema) throws KettleException {
    try {
      pur.deleteFile(idPartitionSchema.getId(), null);
    } catch (Exception e) {
      throw new KettleException("Unable to delete partition schema with name [" + idPartitionSchema + "]", e);
    }
  }

  public void deleteSlave(ObjectId idSlave) throws KettleException {
    try {
      pur.deleteFile(idSlave.getId(), null);
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

    String absolutePath = null;

    if (repositoryDirectory != null) {
      ObjectId id = repositoryDirectory.getObjectId();
      absolutePath = repositoryDirectory.getPath();
      if ((isUserHomeDirectoryAliased) && (id.getId().equals(userHomeAlias.toString()))) {
        absolutePath = RepositoryPaths.getTenantHomeFolderPath().concat(absolutePath);
      } else {
        absolutePath = RepositoryPaths.getTenantRootFolderPath().concat(absolutePath);
      }
    }

    // return the directory path
    if (objectType == null) {
      return absolutePath;
    }

    switch (objectType) {
      case DATABASE: {
        return getDatabaseMetaParentFolderPath() + RepositoryFile.SEPARATOR + name
            + RepositoryObjectType.DATABASE.getExtension();
      }
      case TRANSFORMATION: {
        return absolutePath + RepositoryFile.SEPARATOR + name + RepositoryObjectType.TRANSFORMATION.getExtension();
      }
      case PARTITION_SCHEMA: {
        return getPartitionSchemaParentFolderPath() + RepositoryFile.SEPARATOR + name
            + RepositoryObjectType.PARTITION_SCHEMA.getExtension();
      }
      case SLAVE_SERVER: {
        return getSlaveServerParentFolderPath() + RepositoryFile.SEPARATOR + name
            + RepositoryObjectType.SLAVE_SERVER.getExtension();
      }
      case CLUSTER_SCHEMA: {
        return getClusterSchemaParentFolderPath() + RepositoryFile.SEPARATOR + name
            + RepositoryObjectType.CLUSTER_SCHEMA.getExtension();
      }
      case JOB: {
        return absolutePath + RepositoryFile.SEPARATOR + name + RepositoryObjectType.JOB.getExtension();
      }
      default: {
        throw new UnsupportedOperationException("not implemented");
      }
    }
  }

  public ObjectId getClusterID(String name) throws KettleException {
    try {
      return getObjectId(name, null, RepositoryObjectType.CLUSTER_SCHEMA);
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
        // strip off extension
        names.add(file.getName().substring(0,
            file.getName().length() - RepositoryObjectType.CLUSTER_SCHEMA.getExtension().length()));
      }
      return names.toArray(new String[0]);
    } catch (Exception e) {
      throw new KettleException("Unable to get all cluster schema names", e);
    }
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
    List<Serializable> parentFolderIds = new ArrayList<Serializable>();
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
      pur.deleteFile(fileToDelete.getId(), null);
    } catch (Exception e) {
      throw new KettleException("Unable to delete database with name [" + databaseName + "]", e);
    }
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
      List<RepositoryFile> children = getAllFilesOfType(idDirectory, RepositoryObjectType.JOB, includeDeleted);
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
      absPath = getPath(element.getName(), element.getRepositoryDirectory(), element.getRepositoryElementType());
      RepositoryFile file = pur.getFile(absPath);
      return getRevisions(new StringObjectId(file.getId().toString()));
    } catch (Exception e) {
      throw new KettleException("Could not retrieve version history of object with path [" + absPath + "]", e);
    }
  }

  public RepositorySecurityProvider getSecurityProvider() {
    return securityProvider;
  }

  public RepositorySecurityManager getSecurityManager() {
    return securityManager;
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
        list.add(loadSlaveServer(id, null)); // the last version
      }

      return list;
    } catch (Exception e) {
      throw new KettleException("Unable to load all slave servers from the repository", e);
    }
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
      List<RepositoryFile> children = getAllFilesOfType(idDirectory, RepositoryObjectType.TRANSFORMATION,
          includeDeleted);
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
    return getPdiObjects(idDirectory,
        Arrays.asList(new RepositoryObjectType[] { RepositoryObjectType.TRANSFORMATION }), includeDeleted);
  }

  protected List<RepositoryObject> getPdiObjects(ObjectId dirId, List<RepositoryObjectType> objectTypes,
      boolean includeDeleted) throws KettleException {
    try {

      RepositoryDirectory repDir = getRootDir().findDirectory(dirId);

      List<RepositoryObject> list = new ArrayList<RepositoryObject>();
      List<RepositoryFile> nonDeletedChildren = getAllFilesOfType(dirId, objectTypes);
      for (RepositoryFile file : nonDeletedChildren) {
        RepositoryLock lock = getLock(file);
        String lockMessage = lock == null ? null : lock.getMessage() + " (" + lock.getLogin() + " since "
            + XMLHandler.date2string(lock.getLockDate()) + ")";
        RepositoryObjectType objectType = getObjectType(file.getName());
        list.add(new RepositoryObject(new StringObjectId(file.getId().toString()), file.getName().substring(0,
            file.getName().length() - objectType.getExtension().length()), repDir, null, file.getLastModifiedDate(),
            objectType, null, lockMessage, false));
      }
      if (includeDeleted) {
        List<RepositoryFile> deletedChildren = getAllDeletedFilesOfType(dirId, objectTypes);
        for (RepositoryFile file : deletedChildren) {
          RepositoryLock lock = getLock(file);
          String lockMessage = lock == null ? null : lock.getMessage() + " (" + lock.getLogin() + " since "
              + XMLHandler.date2string(lock.getLockDate()) + ")";
          RepositoryObjectType objectType = getObjectType(file.getName());
          list.add(new RepositoryObject(new StringObjectId(file.getId().toString()), file.getName().substring(0,
              file.getName().length() - objectType.getExtension().length()), repDir, null, file.getLastModifiedDate(),
              objectType, null, lockMessage, true));
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

  public RepositoryVersionRegistry getVersionRegistry() throws KettleException {
    throw new UnsupportedOperationException();
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
      ClusterSchema clusterSchema = new ClusterSchema();
      clusterTransformer.dataNodeToElement(pur.getDataAtVersionForRead(idClusterSchema.getId(), versionId,
          NodeRepositoryFileData.class).getNode(), clusterSchema);
      clusterSchema.setObjectId(new StringObjectId(idClusterSchema));
      clusterSchema.setObjectRevision(getObjectRevision(idClusterSchema, versionId));
      clusterSchema.clearChanged();
      return clusterSchema;
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
      PartitionSchema partitionSchema = new PartitionSchema();
      partitionSchemaTransformer.dataNodeToElement(pur.getDataAtVersionForRead(partitionSchemaId.getId(), versionId,
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
      slaveTransformer.dataNodeToElement(pur.getDataAtVersionForRead(idSlaveServer.getId(), versionId,
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

  public SharedObjects readJobMetaSharedObjects(final JobMeta jobMeta) throws KettleException {
    return jobDelegate.loadSharedObjects(jobMeta);
  }

  public SharedObjects readTransSharedObjects(final TransMeta transMeta) throws KettleException {
    return transDelegate.loadSharedObjects(transMeta);
  }

  public ObjectId renameDatabase(ObjectId idDatabase, String newName) throws KettleException {
    RepositoryFile file = pur.getFileById(idDatabase.getId());
    StringBuilder buf = new StringBuilder(file.getAbsolutePath().length());
    buf.append(getParentPath(file.getAbsolutePath()));
    buf.append(RepositoryFile.SEPARATOR);
    buf.append(newName);
    if (!newName.endsWith(RepositoryObjectType.DATABASE.getExtension())) {
      buf.append(RepositoryObjectType.DATABASE.getExtension());
    }
    pur.moveFile(file.getId(), buf.toString(), null);
    return new StringObjectId(file.getId().toString());
  }

  public ObjectId renameJob(final ObjectId idJob, final RepositoryDirectory newDirectory, final String newName)
      throws KettleException {
    pur.moveFile(idJob.getId(), calcDestAbsPath(idJob, newDirectory, newName, RepositoryObjectType.JOB), null);
    return idJob;
  }

  public ObjectId renameTransformation(final ObjectId idTransformation, final RepositoryDirectory newDirectory,
      final String newName) throws KettleException {
    pur.moveFile(idTransformation.getId(), calcDestAbsPath(idTransformation, newDirectory, newName,
        RepositoryObjectType.TRANSFORMATION), null);
    return idTransformation;
  }

  protected String getParentPath(final String absPath) {
    int lastSlashIndex = absPath.lastIndexOf(RepositoryFile.SEPARATOR);
    return absPath.substring(0, lastSlashIndex);
  }

  protected String calcDestAbsPath(final ObjectId id, final RepositoryDirectory newDirectory, final String newName,
      final RepositoryObjectType objectType) {
    String newDirectoryPath = getPath(null, newDirectory, null);
    RepositoryFile file = pur.getFileById(id.getId());
    StringBuilder buf = new StringBuilder(file.getAbsolutePath().length());
    if (newDirectory != null) {
      buf.append(newDirectoryPath);
    } else {
      buf.append(getParentPath(file.getAbsolutePath()));
    }
    buf.append(RepositoryFile.SEPARATOR);
    if (newName != null) {
      buf.append(newName);
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
      file = pur.createFile(getDatabaseMetaParentFolderId(), file, new NodeRepositoryFileData(databaseMetaTransformer
          .elementToDataNode(element)), versionComment);
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
      databaseMetaTransformer.dataNodeToElement(pur.getDataAtVersionForRead(databaseId.getId(), versionId,
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
      transDelegate.dataNodeToElement(pur
          .getDataAtVersionForRead(file.getId(), versionId, NodeRepositoryFileData.class).getNode(), transMeta);
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
      jobDelegate.dataNodeToElement(pur.getDataAtVersionForRead(file.getId(), versionId, NodeRepositoryFileData.class)
          .getNode(), jobMeta);
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
        file = pur.createFile(getPartitionSchemaParentFolderId(), file, new NodeRepositoryFileData(
            partitionSchemaTransformer.elementToDataNode(element)), versionComment);
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
        file = pur.createFile(getSlaveServerParentFolderId(), file, new NodeRepositoryFileData(slaveTransformer
            .elementToDataNode(element)), versionComment);
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

  protected void saveClusterSchema(final RepositoryElementInterface element, final String versionComment) {
    boolean isUpdate = element.getObjectId() != null;
    RepositoryFile file = null;
    try {
      if (isUpdate) {
        file = pur.getFileById(element.getObjectId().getId());
        file = pur.updateFile(file, new NodeRepositoryFileData(clusterTransformer.elementToDataNode(element)),
            versionComment);
      } else {
        file = new RepositoryFile.Builder(element.getName() + RepositoryObjectType.CLUSTER_SCHEMA.getExtension())
            .versioned(VERSION_SHARED_OBJECTS).build();
        file = pur.createFile(getClusterSchemaParentFolderId(), file, new NodeRepositoryFileData(clusterTransformer
            .elementToDataNode(element)), versionComment);
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

  private ObjectRevision getObjectRevision(final ObjectId elementId, final String versionId) {
    VersionSummary versionSummary = pur.getVersionSummary(elementId.getId(), versionId);
    return new PurObjectRevision(versionSummary.getId(), versionSummary.getAuthor(), versionSummary.getDate(),
        versionSummary.getMessage());
  }

  private String getDatabaseMetaParentFolderPath() {
    return RepositoryPaths.getTenantEtcFolderPath() + RepositoryFile.SEPARATOR + FOLDER_PDI + RepositoryFile.SEPARATOR
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
    return RepositoryPaths.getTenantEtcFolderPath() + RepositoryFile.SEPARATOR + FOLDER_PDI + RepositoryFile.SEPARATOR
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
    return RepositoryPaths.getTenantEtcFolderPath() + RepositoryFile.SEPARATOR + FOLDER_PDI + RepositoryFile.SEPARATOR
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
    return RepositoryPaths.getTenantEtcFolderPath() + RepositoryFile.SEPARATOR + FOLDER_PDI + RepositoryFile.SEPARATOR
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

  public void undeleteObject(final RepositoryElementLocationInterface element) throws KettleException {

    RepositoryFile originalParentFolder = pur.getFile(getPath(null, element.getRepositoryDirectory(), null));
    List<RepositoryFile> deletedChildren = pur.getDeletedFiles(originalParentFolder.getId(), element.getName()
        + element.getRepositoryElementType().getExtension());
    if (!deletedChildren.isEmpty()) {
      pur.undeleteFile(deletedChildren.get(0).getId(), null);
    }
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
        } else if (perm.equals(RepositoryFilePermission.DELETE)) {
          permissionSet.add(ObjectPermission.DELETE);
        } else if (perm.equals(RepositoryFilePermission.DELETE_CHILD)) {
          permissionSet.add(ObjectPermission.DELETE_CHILD);
        } else if (perm.equals(RepositoryFilePermission.EXECUTE)) {
          permissionSet.add(ObjectPermission.EXECUTE);
        } else if (perm.equals(RepositoryFilePermission.READ_ACL)) {
          permissionSet.add(ObjectPermission.READ_ACL);
        } else if (perm.equals(RepositoryFilePermission.WRITE)) {
          permissionSet.add(ObjectPermission.WRITE);
        } else if (perm.equals(RepositoryFilePermission.WRITE_ACL)) {
          permissionSet.add(ObjectPermission.WRITE_ACL);
        } else {
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
            } else if (perm.equals(ObjectPermission.DELETE)) {
              permissionSet.add(RepositoryFilePermission.DELETE);
            } else if (perm.equals(ObjectPermission.DELETE_CHILD)) {
              permissionSet.add(RepositoryFilePermission.DELETE_CHILD);
            } else if (perm.equals(ObjectPermission.EXECUTE)) {
              permissionSet.add(RepositoryFilePermission.EXECUTE);
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

  public List<RepositoryObject> getJobAndTransformationObjects(ObjectId id_directory, boolean includeDeleted)
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

  public List<RepositoryElement> getTrash() throws KettleException {
    List<RepositoryElement> trash = new ArrayList<RepositoryElement>();
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

          public RepositoryDirectory getParent() {
            return origParentDir;
          }
        });
      } else {
        RepositoryObjectType objectType = getObjectType(file.getName());
        trash.add(new RepositoryObject(new StringObjectId(file.getId().toString()), file.getName().substring(0,
            file.getName().length() - objectType.getExtension().length()), origParentDir, null, file.getDeletedDate(),
            objectType, null, null, true));
      }
    }
    return trash;
  }
}
