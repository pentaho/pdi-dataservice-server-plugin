package org.pentaho.di.repository.jcr;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.jcr.ItemNotFoundException;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.PathNotFoundException;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Workspace;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.version.Version;
import javax.jcr.version.VersionException;
import javax.jcr.version.VersionHistory;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitNodeTypeManager;
import org.apache.jackrabbit.rmi.repository.URLRemoteRepository;
import org.pentaho.di.cluster.ClusterSchema;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ProgressMonitorListener;
import org.pentaho.di.core.annotations.RepositoryPlugin;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleSecurityException;
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
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.di.repository.jcr.util.JCRObjectRevision;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.di.trans.TransMeta;


@RepositoryPlugin(
		id="JCRRepository", 
		i18nPackageName="org.pentaho.di.repository.jcr",
		description="JCRRepository.Description", 
		name="JCRRepository.Name", 
		metaClass="org.pentaho.di.repository.jcr.JCRRepositoryMeta",
		dialogClass="org.pentaho.di.ui.repository.jcr.JCRRepositoryDialog",
		versionBrowserClass="org.pentaho.di.ui.repository.jcr.JCRRepositoryRevisionBrowserDialog"

		)
public class JCRRepository implements Repository {
	// private static Class<?> PKG = JCRRepository.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	public static final String PDI_NODE_NAME = "pdi-root";
	
	public static final String NODE_TYPE_UNSTRUCTURED = "nt:unstructured";
	// public static final String NODE_TYPE_FOLDER = "nt:folder";
	public static final String NODE_TYPE_FILE = "nt:file";
	
	public static final String NODE_LOCK_NODE_NAME = "__PDI_LOCKS__";
	
	public static final String	MIX_REFERENCEABLE	= "mix:referenceable";
	public static final String	MIX_LOCKABLE	    = "mix:lockable";
	public static final String	MIX_VERSIONABLE	    = "mix:versionable";


	public static final String	EXT_TRANSFORMATION		= ".ktr";
	public static final String  EXT_JOB					= ".kjb";
	public static final String	EXT_DATABASE			= ".kdb";
	public static final String	EXT_SLAVE_SERVER 		= ".ksl";
	public static final String	EXT_CLUSTER_SCHEMA 		= ".kcs";
	public static final String	EXT_PARTITION_SCHEMA	= ".kps";
	public static final String	EXT_STEP				= ".kst";
	public static final String	EXT_JOB_ENTRY			= ".kje";
	public static final String	EXT_JOB_ENTRY_COPY		= ".kjc";

	public static final String	PROPERTY_NAME       	= "Name";
	public static final String	PROPERTY_DESCRIPTION	= "Description";
	public static final String	PROPERTY_XML	= "XML";
	public static final String	PROPERTY_USER_CREATED = "UserCreated";
	public static final String	PROPERTY_VERSION_COMMENT = "VersionComment";

	public static final String	PROPERTY_PARENT_OBJECT = "ParentObject";
	public static final String	PROPERTY_CHILD_OBJECT = "ChildObject";

	private static final String	PROPERTY_LOCK_OBJECT = "LockObject";
	private static final String	PROPERTY_LOCK_LOGIN = "LockLogin";
	private static final String	PROPERTY_LOCK_USERNAME= "LockUsername";
	private static final String	PROPERTY_LOCK_MESSAGE = "LockMessage";
	private static final String	PROPERTY_LOCK_DATE = "LockDate";
	private static final String	PROPERTY_LOCK_PATH = "LockPath";
	
	public static final String PROPERTY_DELETED = "Deleted";
	
	public static final String PROPERTY_CODE_NR_SEPARATOR = "_#_";

	public static final String NS_PDI = "pdi";


	public static String NODE_TYPE_PDI_FOLDER = "pdifolder";
	private static String CND_TYPE_PDI_FOLDER= "["+NODE_TYPE_PDI_FOLDER+"] > nt:unstructured";

	public static String NODE_TYPE_RELATION = "pdirelation";
	private static String CND_TYPE_RELATION = "["+NODE_TYPE_RELATION+"] > nt:base"+Const.CR+"- "+PROPERTY_DESCRIPTION+" (STRING)"+Const.CR+"- "+PROPERTY_PARENT_OBJECT+" (REFERENCE)"+Const.CR+"- "+PROPERTY_CHILD_OBJECT+" (REFERENCE)";

	public static String NODE_TYPE_PDI_LOCK = "pdilock";
	private static String CND_TYPE_PDI_LOCK = "["+NODE_TYPE_PDI_LOCK+"] > nt:base"+Const.CR+"- "+PROPERTY_LOCK_OBJECT+" (REFERENCE)"+Const.CR+"- "+PROPERTY_LOCK_LOGIN+" (STRING)"+Const.CR+"- "+PROPERTY_LOCK_USERNAME+" (STRING)"+Const.CR+"- "+PROPERTY_LOCK_PATH+" (STRING)"+Const.CR+"- "+PROPERTY_LOCK_MESSAGE+" (STRING)"+Const.CR+"- "+PROPERTY_LOCK_DATE+" (DATE)";

	private JCRRepositoryMeta jcrRepositoryMeta;
	private UserInfo userInfo;
	private JCRRepositorySecurityProvider	securityProvider;
	
	private JCRRepositoryLocation	repositoryLocation;
	private URLRemoteRepository	jcrRepository;
	private Session	session;
	private Workspace	workspace;
	private JackrabbitNodeTypeManager nodeTypeManager;
	private Node	rootNode;

	private Node	lockNodeFolder;

	private JCRRepositoryTransDelegate transDelegate;
	private JCRRepositoryDatabaseDelegate	databaseDelegate;
	private JCRRepositoryPartitionDelegate	partitionDelegate;
	private JCRRepositorySlaveDelegate	slaveDelegate;
	private JCRRepositoryJobDelegate	jobDelegate;
	
	public JCRRepository() {
		this.transDelegate = new JCRRepositoryTransDelegate(this);
		this.jobDelegate = new JCRRepositoryJobDelegate(this);
		this.databaseDelegate = new JCRRepositoryDatabaseDelegate(this);
		this.partitionDelegate = new JCRRepositoryPartitionDelegate(this);
		this.slaveDelegate = new JCRRepositorySlaveDelegate(this);
	}
	
	public String getName() {
		return jcrRepositoryMeta.getName();
	}

	public String getVersion() {
		return jcrRepository.getDescriptor(javax.jcr.Repository.SPEC_VERSION_DESC);
	}

	public void init(RepositoryMeta repositoryMeta, UserInfo userInfo) {
		this.jcrRepositoryMeta = (JCRRepositoryMeta)repositoryMeta;
		this.userInfo = userInfo;
		this.repositoryLocation = jcrRepositoryMeta.getRepositoryLocation();		
		this.securityProvider = new JCRRepositorySecurityProvider(this, repositoryMeta, userInfo);
	}

	public void connect() throws KettleException, KettleSecurityException {
		try {
			jcrRepository = new URLRemoteRepository(repositoryLocation.getUrl());
			
			session = jcrRepository.login(new SimpleCredentials(userInfo.getLogin(), userInfo.getPassword()!=null ? userInfo.getPassword().toCharArray() : "".toCharArray() ));
			workspace = session.getWorkspace();
			
			rootNode = session.getRootNode();
			
			nodeTypeManager = (JackrabbitNodeTypeManager) workspace.getNodeTypeManager();
			
			if (!nodeTypeManager.hasNodeType(NODE_TYPE_PDI_FOLDER)) {
				nodeTypeManager.registerNodeTypes(new ByteArrayInputStream(CND_TYPE_PDI_FOLDER.getBytes(Const.XML_ENCODING)), JackrabbitNodeTypeManager.TEXT_X_JCR_CND);
			}
			if (!nodeTypeManager.hasNodeType(NODE_TYPE_RELATION)) {
				nodeTypeManager.registerNodeTypes(new ByteArrayInputStream(CND_TYPE_RELATION.getBytes(Const.XML_ENCODING)), JackrabbitNodeTypeManager.TEXT_X_JCR_CND);
			}
			if (!nodeTypeManager.hasNodeType(NODE_TYPE_PDI_LOCK)) {
				nodeTypeManager.registerNodeTypes(new ByteArrayInputStream(CND_TYPE_PDI_LOCK.getBytes(Const.XML_ENCODING)), JackrabbitNodeTypeManager.TEXT_X_JCR_CND);
			}
			

			String[] prefixes = session.getNamespacePrefixes();
			if (Const.indexOfString(NS_PDI, prefixes)<0) {
				// TODO: create a pdi namespace...
				//
			}

			// Create a folder, type unstructured in the root to store the locks...
			//
			lockNodeFolder = null;
			NodeIterator nodes = rootNode.getNodes();
			while (nodes.hasNext() && lockNodeFolder==null) {
				Node node = nodes.nextNode();
				if (node.getName().equals(NODE_LOCK_NODE_NAME)) {
					lockNodeFolder = node;
				}
			}
			if (lockNodeFolder==null) {
				lockNodeFolder = rootNode.addNode(NODE_LOCK_NODE_NAME, "nt:unstructured");
			}
			
		} catch(Exception e) {
			session=null;
			throw new KettleException("Unable to connect to JCR repository on URL: "+repositoryLocation.getUrl(), e);
		}
	}
	
	public void disconnect() {
		session.logout();
		session=null;
	}

	public boolean isConnected() {
		return session!=null;
	}

	//
	// Directories
	//
	
	Node findFolderNode(RepositoryDirectory dir) throws KettleException {
		
		if (dir.isRoot()) {
			return rootNode; 
		}
		
		String path = dir.getPath();
		String relPath = path.substring(1);
		
		try {
			return rootNode.getNode(relPath);
		} catch(Exception e) {
			throw new KettleException("Unable to find folder node with path ["+dir.getPath()+"]", e);
		}
	}

	public RepositoryDirectory loadRepositoryDirectoryTree() throws KettleException {
		try {
			RepositoryDirectory root = new RepositoryDirectory();
			loadRepositoryDirectory(root, rootNode);
			root.setObjectId(null);
			return root;
		} catch (Exception e) {
			throw new KettleException("Unable to load directory tree from the JCR repository", e);
		}
	}
	
	private RepositoryDirectory loadRepositoryDirectory(RepositoryDirectory root, Node node) throws KettleException {
		try {
			NodeIterator nodes = node.getNodes();
			while (nodes.hasNext()) {
				Node subNode = nodes.nextNode();
				if (subNode.isNodeType(NODE_TYPE_PDI_FOLDER)) {
					RepositoryDirectory dir = new RepositoryDirectory();
					dir.setDirectoryName(subNode.getName());
					dir.setObjectId(new StringObjectId(subNode.getUUID()));
					root.addSubdirectory(dir);
					
					loadRepositoryDirectory(dir, subNode);
				}
			}
			
			return root;
		} catch (Exception e) {
			throw new KettleException("Unable to load directory structure from JCR repository", e);
		}
	}

	public void saveRepositoryDirectory(RepositoryDirectory dir) throws KettleException {
		try {
			Node parentNode = findFolderNode(dir.getParent());
			Node node = parentNode.addNode(dir.getDirectoryName(), NODE_TYPE_PDI_FOLDER);
			node.addMixin(MIX_REFERENCEABLE);
			node.addMixin(MIX_LOCKABLE);
			session.save();
			dir.setObjectId(new StringObjectId(node.getUUID()));
		} catch(Exception e) {
			throw new KettleException("Unable to save repository directory with path ["+dir.getPath()+"]", e);
		}
	}
	
	public RepositoryDirectory createRepositoryDirectory(RepositoryDirectory parent, String directoryPath) throws KettleException {
		try {
			String[] path = Const.splitPath(directoryPath, RepositoryDirectory.DIRECTORY_SEPARATOR);
			
			RepositoryDirectory follow = parent;
		    for (int level=0;level<path.length;level++)
		    {
		    	RepositoryDirectory child = follow.findChild(path[level]);
		    	if (child==null) {
		    		// create this one
		    		//
		    		child = new RepositoryDirectory(follow, path[level]);
		    		saveRepositoryDirectory(child);
		    	} 
		    	
		    	follow = child;
		    }
		    return follow;
		} catch(Exception e) {
			throw new KettleException("Unable to create directory with path ["+directoryPath+"]", e);
		}
	}
	
	public String[] getDirectoryNames(ObjectId id_directory) throws KettleException {
		return getObjectNames(id_directory, null, true);
	}


	
	
	
	
	
	
	private String calcDirectoryPath(RepositoryDirectory dir) {
		if (dir!=null) {
			return dir.getPath();
		} else {
			return "/";
		}
	}
	
	public String calcNodePath(RepositoryDirectory directory, String name, String extension) {
		StringBuilder path = new StringBuilder();
		
		String dirPath = calcDirectoryPath(directory);
		path.append(dirPath);
		if (!dirPath.endsWith("/")) {
			path.append("/");
		}
		
		path.append(name+extension);
		
		return path.toString();
	}
	
	public String calcRelativeNodePath(RepositoryDirectory directory, String name, String extension) {
		return calcNodePath(directory, name, extension).substring(1); // skip 1
	}

	public String calcNodePath(RepositoryElementInterface element) {
		RepositoryDirectory directory = element.getRepositoryDirectory();
		String name = element.getName();
		String extension = element.getRepositoryElementType().getExtension();
		
		return calcNodePath(directory, name, extension);
	}
		
	// General 
	//
	public void save(RepositoryElementInterface element, String versionComment, ProgressMonitorListener monitor) throws KettleException {
		try {
			switch(element.getRepositoryElementType()) {
			case TRANSFORMATION : transDelegate.saveTransMeta(element, versionComment, monitor); break;
			case JOB: jobDelegate.saveJobMeta(element, versionComment, monitor); break;
			case DATABASE : databaseDelegate.saveDatabaseMeta(element, versionComment, monitor); break;
			case SLAVE_SERVER : slaveDelegate.saveSlaveServer(element, versionComment, monitor); break;
			case CLUSTER_SCHEMA : slaveDelegate.saveClusterSchema(element, versionComment, monitor); break;
			case PARTITION_SCHEMA : partitionDelegate.savePartitionSchema(element, versionComment, monitor); break;
			default: 
				throw new KettleException("It's not possible to save Class ["+element.getClass().getName()+"] to the JCR Repository");
			}
		} catch(Exception e) {
			throw new KettleException("Unable to save repository element ["+element+"]", e);
		}
	}
	
	
	public Node saveAsXML(String xml, RepositoryElementInterface element, String versionComment, ProgressMonitorListener monitor, boolean checkIn) throws KettleException {
		
		if (element instanceof DatabaseMeta) {
			 throw new KettleException("Please use save() or saveDatabaseMeta for databases");
		}
		
		try {
			
			Node node = createOrVersionNode(element, versionComment);
			ObjectId id = new StringObjectId(node.getUUID());
	
			node.setProperty(PROPERTY_XML, xml);
			
			session.save();

			if (checkIn) {
				Version version = node.checkin();
				element.setObjectRevision(new JCRObjectRevision(version, versionComment, userInfo.getLogin()));	
			}
			
			element.setObjectId(id);
					
			return node;
		}
		catch(Exception e) {
			throw new KettleException("Unable to save repository element ["+element+"] to the JCR repository as XML", e);
		}

	}

	public void unlockTransformation(ObjectId transformationId) throws KettleException {
		try {
			Node node = session.getNodeByUUID(transformationId.getId());
			unlockNode(node); 
		} catch(Exception e) {
			throw new KettleException("Unable to unlock transformation with id ["+transformationId+"]", e);
		}
	}

	public void lockTransformation(ObjectId transformationId, String message) throws KettleException {
		try {
			lockNode(transformationId, message);
		} catch(Exception e) {
			throw new KettleException("Unable to lock transformation with id ["+transformationId+"]", e);
		}
	}

	/**
	 * @param objectId
	 * @param isSessionScoped
	 * @param message
	 * @throws KettleException
	 */
	private void lockNode(ObjectId objectId, String message) throws KettleException {
		try {
			Node node = session.getNodeByUUID(objectId.getId());
			
			// There are a few bugs in the Jackrabbit locking implementation.
			// https://issues.apache.org/jira/browse/JCR-1634
			// It's marked as fixed in the next release, but that one's still in alpha release.
			//
			
			Node lockNode = lockNodeFolder.addNode(objectId.getId(), NODE_TYPE_PDI_LOCK);
			lockNode.setProperty(PROPERTY_LOCK_OBJECT, node);
			lockNode.setProperty(PROPERTY_LOCK_MESSAGE, message);
			lockNode.setProperty(PROPERTY_LOCK_DATE, Calendar.getInstance());
			lockNode.setProperty(PROPERTY_LOCK_LOGIN, userInfo.getLogin());
			lockNode.setProperty(PROPERTY_LOCK_USERNAME, userInfo.getUsername());
			lockNode.setProperty(PROPERTY_LOCK_PATH, node.getPath());
			
			session.save();
		} catch(Exception e) {
			throw new KettleException("Unable to lock node with id ["+objectId+"]", e);
		}
	}

	void unlockNode(Node node) throws KettleException {
		try {
			
			String uuid = node.getUUID();
			Node lockNode = lockNodeFolder.getNode(uuid);
			lockNode.remove();
			
			session.save();
		} catch(Exception e) {
			throw new KettleException("Unable to unlock node", e);
		}
	}

	public boolean exists(String name, RepositoryDirectory repositoryDirectory, RepositoryObjectType objectType) throws KettleException {
		try {
			Node node = getNode(name, repositoryDirectory, objectType.getExtension());
			if (node==null) return false;
			
			if (getPropertyBoolean(node, PROPERTY_DELETED, false)) {
				return false;
			}
			
			return true;
		} catch(Exception e) {
			throw new KettleException("Unable to verify if the repository element ["+name+"] exists in ", e);
		}
	}

	
	public TransMeta loadTransformation(String transname, RepositoryDirectory repdir, ProgressMonitorListener monitor, boolean setInternalVariables, String versionLabel) throws KettleException {
		return transDelegate.loadTransformation(transname, repdir, monitor, setInternalVariables, versionLabel);
	}
	
	public SharedObjects readTransSharedObjects(TransMeta transMeta) throws KettleException {
		return transDelegate.readTransSharedObjects(transMeta);
	}

	
	
	public Version getLastVersion(Node node) throws UnsupportedRepositoryOperationException, RepositoryException {
		
		VersionHistory versionHistory = node.getVersionHistory();
		Version version = versionHistory.getRootVersion();
		
		Version[] successors = version.getSuccessors();
		
		while (successors!=null && successors.length>0) {
			version = successors[0];
			successors = version.getSuccessors();
		}
		return version;
	}
	
	Node getVersionNode(Version version) throws PathNotFoundException, RepositoryException {
		return version.getNode(JcrConstants.JCR_FROZENNODE);
	}
	
	
	
	
	
	
	
	
	
	

	public void deleteJob(ObjectId jobId) throws KettleException {
		jobDelegate.deleteJob(jobId);
	}
	
	public void removeVersion(ObjectId id, String version) throws KettleException {
		try {
			Node node = session.getNodeByUUID(id.getId());
			VersionHistory versionHistory = node.getVersionHistory();
			versionHistory.removeVersion(version);
		} catch(Exception e) {
			throw new KettleException("Unable to remove last version of object with ID ["+id+"]", e);
		}
	}

	public void deleteTransformation(ObjectId transformationId) throws KettleException {
		transDelegate.deleteTransformation(transformationId);
	}

	public void deleteClusterSchema(ObjectId clusterId) throws KettleException {
		slaveDelegate.deleteClusterSchema(clusterId);
	}

	public void deletePartitionSchema(ObjectId partitionSchemaId) throws KettleException {
		try {
			deleteObject(partitionSchemaId, RepositoryObjectType.PARTITION_SCHEMA);
		} catch(Exception e) {
			throw new KettleException("Unable to delete partition schema with id ["+partitionSchemaId+"]", e);
		}
	}

	public void deleteRepositoryDirectory(RepositoryDirectory dir) throws KettleException {
		try {
			Node dirNode = findFolderNode(dir);
			dirNode.remove();
			session.save();
		} catch(Exception e) {
			throw new KettleException("Unable to delete directory with path ["+dir.getPath()+"]", e);
		}
	}

	public void deleteSlave(ObjectId slaveServerId) throws KettleException {
		slaveDelegate.deleteSlave(slaveServerId);
	}

	public void deleteDatabaseMeta(String databaseName) throws KettleException {
		databaseDelegate.deleteDatabaseMeta(databaseName);
	}

	public ObjectId getClusterID(String name) throws KettleException {
		return getObjectId(name, null, EXT_SLAVE_SERVER);
	}

	public ObjectId[] getClusterIDs(boolean includeDeleted) throws KettleException {
		return getObjectIDs((ObjectId)null, EXT_CLUSTER_SCHEMA, includeDeleted);
	}

	public String[] getClusterNames(boolean includeDeleted) throws KettleException {
		return getObjectNames(null, EXT_CLUSTER_SCHEMA, includeDeleted);
	}

	public ObjectId getDatabaseID(String name) throws KettleException {
		return getObjectId(name, null, EXT_DATABASE);
	}

	public ObjectId[] getDatabaseIDs(boolean includeDeleted) throws KettleException {
		return getObjectIDs((ObjectId)null, EXT_DATABASE, includeDeleted);
	}

	public String[] getDatabaseNames(boolean includeDeleted) throws KettleException {
		return getObjectNames(null, EXT_DATABASE, includeDeleted);
	}

	public ObjectId getJobId(String name, RepositoryDirectory repositoryDirectory) throws KettleException {
		return getObjectId(name, repositoryDirectory, EXT_JOB);
	}

	public RepositoryLock getJobLock(ObjectId jobId) throws KettleException {
		return getLock(jobId);
	}

	public String[] getJobNames(ObjectId id_directory, boolean includeDeleted) throws KettleException {
		return getObjectNames(id_directory, EXT_JOB, includeDeleted);
	}

	public List<RepositoryObject> getJobObjects(ObjectId id_directory, boolean includeDeleted) throws KettleException {
		return getPdiObjects(id_directory, EXT_JOB, RepositoryObject.STRING_OBJECT_TYPE_JOB, includeDeleted);
	}

	public ObjectId getPartitionSchemaID(String name) throws KettleException {
		return getObjectId(name, null, EXT_PARTITION_SCHEMA);
	}

	public ObjectId[] getPartitionSchemaIDs(boolean includeDeleted) throws KettleException {
		return getObjectIDs((ObjectId)null, EXT_PARTITION_SCHEMA, includeDeleted);
	}

	public String[] getPartitionSchemaNames(boolean includeDeleted) throws KettleException {
		return getObjectNames(null, EXT_PARTITION_SCHEMA, includeDeleted);
	}

	public RepositoryMeta getRepositoryMeta() {
		return jcrRepositoryMeta;
	}

	public RepositorySecurityProvider getSecurityProvider() {
		return securityProvider;
	}

	public ObjectId getSlaveID(String name) throws KettleException {
		return getObjectId(name, null, EXT_SLAVE_SERVER);
	}

	public ObjectId[] getSlaveIDs(boolean includeDeleted) throws KettleException {
		return getObjectIDs((ObjectId)null, EXT_SLAVE_SERVER, includeDeleted);
	}

	public String[] getSlaveNames(boolean includeDeleted) throws KettleException {
		return getObjectNames(null, EXT_SLAVE_SERVER, includeDeleted);
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
		}
	}
	
	public ObjectId getObjectId(RepositoryElementLocationInterface element) throws KettleException {
		return getObjectId(element.getName(), element.getRepositoryDirectory(), element.getRepositoryElementType().getExtension());
	}
	
	/**
	 * Find the object ID of an object.
	 * 
	 * @param name
	 * @param repositoryDirectory
	 * @param extension
	 * 
	 * @return the object ID (UUID) or null if the node couldn't be found.
	 * 
	 * @throws KettleException In case something went horribly wrong
	 */
	public ObjectId getObjectId(String name, RepositoryDirectory repositoryDirectory, String extension) throws KettleException {
		try {
			Node node = getNode(sanitizeNodeName(name), repositoryDirectory, extension);
			if (node==null) return null;
			return new StringObjectId(node.getUUID());
		} catch(Exception e) {
			throw new KettleException("Unable to get ID for object ["+sanitizeNodeName(name)+"] + in directory ["+repositoryDirectory+"] with extension ["+extension+"]", e);
		}
	}
	
	public Node getNode(String name, RepositoryDirectory repositoryDirectory, String extension) throws KettleException {
		String path = calcRelativeNodePath(repositoryDirectory, name, extension);
		try {
			return rootNode.getNode(path);
		} catch(PathNotFoundException e) {
			return null; // Not found!
		} catch(Exception e) {
			throw new KettleException("Unable to get node for object ["+path+"]", e);
		}
		
	}

	public ObjectId[] getObjectIDs(RepositoryDirectory repositoryDirectory, String extension) throws KettleException {

		String path = repositoryDirectory.getPath();
		try {

			Node folderNode;
			if (path.length()<=1) {
				folderNode = rootNode;
			} else {
				folderNode = rootNode.getNode(path.substring(1));
			}
			
			List<ObjectId> list = new ArrayList<ObjectId>();
			NodeIterator nodeIterator = folderNode.getNodes();
			
			while (nodeIterator.hasNext()) {
				Node node = nodeIterator.nextNode();
				if (Const.isEmpty(extension)) {
					if (node.isNodeType(NODE_TYPE_PDI_FOLDER)) {
						list.add(new StringObjectId(node.getUUID()));
					}
				} else {
					if (node.isNodeType(NODE_TYPE_UNSTRUCTURED)) {
						if (node.getName().endsWith(extension)) {
							list.add(new StringObjectId(node.getUUID()));
						}
					}
				}
			}
			return list.toArray(new ObjectId[list.size()]);
		} catch(Exception e) {
			throw new KettleException("Unable to get ID for object ["+path+"]", e);
		}
	}
	
	public ObjectId[] getObjectIDs(ObjectId id_directory, String extension, boolean includeDeleted) throws KettleException {

		try {

			Node folderNode;
			if (id_directory==null) {
				folderNode = rootNode;
			} else {
				folderNode = session.getNodeByUUID(id_directory.getId());
			}

			return getObjectIDs(folderNode, extension, includeDeleted);
		} catch(Exception e) {
			throw new KettleException("Unable to get ID for object ["+id_directory+"]", e);
		}
	}

	private ObjectId[] getObjectIDs(Node folderNode, String extension, boolean includeDeleted) throws KettleException {
		try {
			List<ObjectId> list = new ArrayList<ObjectId>();
			NodeIterator nodeIterator = folderNode.getNodes();
			
			while (nodeIterator.hasNext()) {
				Node node = nodeIterator.nextNode();
				if (Const.isEmpty(extension)) {
					if (node.isNodeType(NODE_TYPE_PDI_FOLDER)) {
						list.add(new StringObjectId(node.getUUID()));
					}
				} else {
					if (node.isNodeType(NODE_TYPE_UNSTRUCTURED)) {
						if (node.getName().endsWith(extension)) {
							
							// See if the node is deleted or not.
							//
							if (includeDeleted || !getPropertyBoolean(node, PROPERTY_DELETED, false)) {
								list.add(new StringObjectId(node.getUUID()));
							}
						}
					}
				}
			}
			return list.toArray(new ObjectId[list.size()]);
		}
		catch(Exception e) {
			throw new KettleException("Unable to get object IDs from folder node ["+folderNode+getName()+"]", e);
		}
	}

	public ObjectId getTransformationID(String name, RepositoryDirectory repositoryDirectory) throws KettleException {
		return getObjectId(name, repositoryDirectory, EXT_TRANSFORMATION);
	}

	public RepositoryLock getTransformationLock(ObjectId transformationId) throws KettleException {
		return getLock(transformationId);
	}
	
	RepositoryLock getLock(ObjectId objectId) throws KettleException {
		try {
			
			Node lockNode = lockNodeFolder.getNode(objectId.getId());
			String message = lockNode.getProperty(PROPERTY_LOCK_MESSAGE).getString();
			String login = lockNode.getProperty(PROPERTY_LOCK_LOGIN).getString();
			String username = lockNode.getProperty(PROPERTY_LOCK_USERNAME).getString();
			String path = lockNode.getProperty(PROPERTY_LOCK_PATH).getString();
			Date lockDate = lockNode.getProperty(PROPERTY_LOCK_DATE).getDate().getTime();
			Node parent = lockNode.getProperty(PROPERTY_LOCK_OBJECT).getNode();
			
			// verify node path with lock
			//
			if (path!=null && !path.equals(parent.getPath())) {
				throw new KettleException("Problem found in locking system, referenced node path ["+parent.getPath()+"] is not the same as the stored path ["+path+"]");
			}
			
			return new RepositoryLock(objectId, message, login, username, lockDate); 
		} catch(PathNotFoundException e) {
			return null; // NOT FOUND!
		}
		catch(Exception e) {
			throw new KettleException("Unable to get lock status for object ["+objectId+"]", e);
		}
	}

	private String[] getObjectNames(ObjectId id_directory, String extension, boolean includeDeleted) throws KettleException {
		try {
			Node folderNode;
			if (id_directory==null) {
				folderNode = rootNode;
			} else {
				folderNode = session.getNodeByUUID(id_directory.getId());
			}
			List<String> names = new ArrayList<String>();
			NodeIterator nodes = folderNode.getNodes();
			while (nodes.hasNext()) {
				Node node = nodes.nextNode();
				
				if (Const.isEmpty(extension)) {
					// Folders
					//
					if (node.isNodeType(NODE_TYPE_PDI_FOLDER)) {
						names.add(node.getName());
					}
				} else {
					// Normal Objects
					//
					if (node.isNodeType(NODE_TYPE_UNSTRUCTURED)) {
						if (includeDeleted || !getPropertyBoolean(node, PROPERTY_DELETED)) {
							String fullname = node.getName();
							if (fullname.endsWith(extension)) {
								names.add( getObjectName(node) );
							}
						}
					}
				}
				
			}
			
			return names.toArray(new String[names.size()]);
		}catch(Exception e) {
			throw new KettleException("Unable to get list of object names from directory ["+id_directory+"]", e);
		}
	}

	public String[] getTransformationNames(ObjectId id_directory, boolean includeDeleted) throws KettleException {
		return getObjectNames(id_directory, EXT_TRANSFORMATION, includeDeleted);
	}

	public List<RepositoryObject> getTransformationObjects(ObjectId id_directory, boolean includeDeleted) throws KettleException {
		return getPdiObjects(id_directory, EXT_TRANSFORMATION, RepositoryObject.STRING_OBJECT_TYPE_TRANSFORMATION, includeDeleted);
	}

	private List<RepositoryObject> getPdiObjects(ObjectId id_directory, String extension, String objectType, boolean includeDeleted) throws KettleException {
		
		List<RepositoryObject> list = new ArrayList<RepositoryObject>();
		try {
			ObjectId[] ids = getObjectIDs(id_directory, extension, includeDeleted);
			for (ObjectId objectId : ids) {
				Node transNode = session.getNodeByUUID(objectId.getId());
				Version version = getLastVersion(transNode);

				String name = transNode.getName();
				String description = transNode.getProperty(PROPERTY_DESCRIPTION).getString() + " - v"+version.getName();
				String userCreated = transNode.getProperty(PROPERTY_USER_CREATED).getString();
				Date dateCreated = version.getCreated().getTime();
				
				RepositoryLock lock = getLock(objectId);
				String lockMessage = lock==null ? null : lock.getMessage()+" ("+lock.getLogin()+" since "+XMLHandler.date2string(lock.getLockDate())+")";
				
				list.add(new RepositoryObject(name.substring(0, name.length()-extension.length()), userCreated, dateCreated, objectType, description, lockMessage));
				
			}
			return list;
		}
		catch(Exception e) {
			throw new KettleException("Unable to get list of transformations from directory ["+id_directory+"]", e);
		}
	}



	public UserInfo getUserInfo() {
		return userInfo;
	}

	public void insertJobEntryDatabase(ObjectId jobId, ObjectId jobEntryId, ObjectId databaseId) throws KettleException {
		try {
			Node jobEntryNode = session.getNodeByUUID(jobEntryId.getId());
			Node databaseNode = session.getNodeByUUID(databaseId.getId());
			jobEntryNode.setProperty(databaseId.getId(), databaseNode);
		} catch(Exception e) {
			throw new KettleException("Unable to save step-database relationship!", e);
		}
	}

	public ObjectId insertLogEntry(String description) throws KettleException {
		return null; // TODO!
	}

	public void insertStepDatabase(ObjectId transformationId, ObjectId stepId, ObjectId databaseId) throws KettleException {
		
		if (databaseId==null) return; // Nothing to do here!
		
		try {
			Node stepNode = session.getNodeByUUID(stepId.getId());
			Node databaseNode = session.getNodeByUUID(databaseId.getId());
			stepNode.setProperty(databaseId.getId(), databaseNode);
		} catch(Exception e) {
			throw new KettleException("Unable to save step-database relationship!", e);
		}
	}


	public ClusterSchema loadClusterSchema(ObjectId clusterSchemaId, List<SlaveServer> slaveServers, String versionLabel) throws KettleException {
		return slaveDelegate.loadClusterSchema(clusterSchemaId, slaveServers, versionLabel);
	}

	public DatabaseMeta loadDatabaseMeta(ObjectId databaseId, String versionLabel) throws KettleException {
		return databaseDelegate.loadDatabaseMeta(databaseId, versionLabel);
	}

	public JobMeta loadJob(String jobname, RepositoryDirectory repdir, ProgressMonitorListener monitor, String versionLabel) throws KettleException {
		return jobDelegate.loadJobMeta(jobname, repdir, monitor, versionLabel);
	}

	public PartitionSchema loadPartitionSchema(ObjectId partitionSchemaId, String versionLabel) throws KettleException {
		return partitionDelegate.loadPartitionSchema(partitionSchemaId, versionLabel);
	}
	
	public SlaveServer loadSlaveServer(ObjectId slaveServerId, String versionLabel) throws KettleException {
		return slaveDelegate.loadSlaveServer(slaveServerId, versionLabel);
	}

	public void lockJob(ObjectId jobId, String message) throws KettleException {
		try {
			lockNode(jobId, message);
		} catch(Exception e) {
			throw new KettleException("Unable to lock job with id ["+jobId+"]", e);
		}
	}


	public List<DatabaseMeta> readDatabases() throws KettleException {
		ObjectId[] ids = getDatabaseIDs(false);
		List<DatabaseMeta> list = new ArrayList<DatabaseMeta>();
		for (ObjectId objectId : ids) {
			list.add(loadDatabaseMeta(objectId, null)); // Load the last version
		}
		return list;
	}

	public SharedObjects readJobMetaSharedObjects(JobMeta jobMeta) throws KettleException {
		return jobDelegate.readSharedObjects(jobMeta);
	}

	public ObjectId renameDatabase(ObjectId databaseId, String newname) throws KettleException {
		return databaseDelegate.renameDatabase(databaseId, newname);
	}

	public ObjectId renameJob(ObjectId jobId, RepositoryDirectory newDirectory, String newName) throws KettleException {
		return jobDelegate.renameJob(jobId, newDirectory, newName);
	}

	public ObjectId renameRepositoryDirectory(RepositoryDirectory dir) throws KettleException {
		try {
			Node folderNode = session.getNodeByUUID(dir.getObjectId().getId());
			String parentPath = folderNode.getParent().getPath();
			
			session.move(folderNode.getPath(), parentPath+"/"+dir.getDirectoryName());
			
			return dir.getObjectId();
		} catch(Exception e) {
			throw new KettleException("Unable to rename directory with id ["+dir.getObjectId()+"] to ["+dir.getDirectoryName()+"]", e);
		}
	}

	public ObjectId renameTransformation(ObjectId transformationId, RepositoryDirectory newDirectory, String newName) throws KettleException {
		return transDelegate.renameTransformation(transformationId, newDirectory, newName);
	}

	public void saveConditionStepAttribute(ObjectId transformationId, ObjectId stepId, String code, Condition condition) throws KettleException {
		try {
			Node stepNode = session.getNodeByUUID(stepId.getId());
			
			Node conditionNode = stepNode.addNode(code);
			conditionNode.setProperty(PROPERTY_XML, condition.getXML());
			
			condition.setObjectId( new StringObjectId(conditionNode.getUUID()));
		} catch(Exception e) {
			throw new KettleException("Unable to save condition ["+condition+"] in the repository", e);
		}
	}

	public Condition loadConditionFromStepAttribute(ObjectId stepId, String code) throws KettleException {
		try {
			Node stepNode = session.getNodeByUUID(stepId.getId());
			Node conditionNode = stepNode.getNode(code);
			String xml = conditionNode.getProperty(PROPERTY_XML).getString();
			Condition condition = new Condition( XMLHandler.getSubNode(XMLHandler.loadXMLString(xml), Condition.XML_TAG) );
			return condition;
		} catch(Exception e) {
			throw new KettleException("Unable to load condition from step id ["+stepId+"] with code ["+code+"]", e);
		}
	}

	// Save/Load database from step/jobentry attribute
	
	public void saveDatabaseMetaJobEntryAttribute(ObjectId jobId, ObjectId jobEntryId, String nameCode, String idCode, DatabaseMeta database) throws KettleException {
		try {
			if (database!=null && database.getObjectId()!=null) {
				Node node = session.getNodeByUUID(database.getObjectId().getId());
				saveStepAttribute(jobId, jobEntryId, idCode, node);
			}
		} catch(Exception e) {
			throw new KettleException("Unable to save database reference as a job entry attribute for job entry id ["+jobEntryId+"] and ID code ["+idCode+"]", e);
		}
	}

	public void saveDatabaseMetaStepAttribute(ObjectId transformationId, ObjectId stepId, String code, DatabaseMeta database) throws KettleException {
		try {
			if (database!=null && database.getObjectId()!=null) {
				Node node = session.getNodeByUUID(database.getObjectId().getId());
				saveStepAttribute(transformationId, stepId, code, node);
			}
		} catch(Exception e) {
			throw new KettleException("Unable to save database reference as a step attribute for step id ["+stepId+"] and code ["+code+"]", e);
		}
	}

	public DatabaseMeta loadDatabaseMetaFromJobEntryAttribute(ObjectId jobEntryId, String nameCode, String idCode, List<DatabaseMeta> databases) throws KettleException {
		try {
			
			// We only use links by reference here...
			//
			if (idCode!=null) {
				Node node = getJobEntryAttributeNode(jobEntryId, idCode);
				if (node!=null) {
					ObjectId databaseId = new StringObjectId(node.getUUID());
					return DatabaseMeta.findDatabase(databases, databaseId);
				}
			}
			return null;
		} catch(Exception e) {
			throw new KettleException("Unable to load database reference from a job entry attribute for job entry id ["+jobEntryId+"] and id code ["+idCode+"], name code ["+nameCode+"]", e);
		}
	}
	
	public DatabaseMeta loadDatabaseMetaFromStepAttribute(ObjectId stepId, String code, List<DatabaseMeta> databases) throws KettleException {
		try {
			Node node = getStepAttributeNode(stepId, code);
			if (node==null) {
				return null;
			}
			ObjectId databaseId = new StringObjectId(node.getUUID());
			
			return DatabaseMeta.findDatabase(databases, databaseId);
		} catch(Exception e) {
			throw new KettleException("Unable to save database reference as a step attribute for step id ["+stepId+"] and code ["+code+"]", e);
		}
	}


	
	// JOB ENTRY ATTRIBUTES

	/**
	 * Special edition for JCR nodes, creates a reference to a node
	 */
	public void saveJobEntryAttribute(ObjectId jobId, ObjectId jobEntryId, String code, Node node) throws KettleException {
		try {
			session.getNodeByUUID(jobEntryId.getId()).setProperty(code, node);
		} catch(Exception e) {
			throw new KettleException("Error saving job entry node reference attribute ["+code+"] for job entry with id ["+jobEntryId+"]", e);
		}
	}

	public void saveJobEntryAttribute(ObjectId jobId, ObjectId jobEntryId, String code, String value) throws KettleException {
		try {
			session.getNodeByUUID(jobEntryId.getId()).setProperty(code, value);
		} catch(Exception e) {
			throw new KettleException("Error saving job entry attribute ["+code+"] for job entry with id ["+jobEntryId+"]", e);
		}
	}

	public void saveJobEntryAttribute(ObjectId jobId, ObjectId jobEntryId, String code, boolean value) throws KettleException {
		try {
			session.getNodeByUUID(jobEntryId.getId()).setProperty(code, value);
		} catch(Exception e) {
			throw new KettleException("Error saving job entry attribute ["+code+"] for job entry with id ["+jobEntryId+"]", e);
		}
	}

	public void saveJobEntryAttribute(ObjectId jobId, ObjectId jobEntryId, String code, long value) throws KettleException {
		try {
			session.getNodeByUUID(jobEntryId.getId()).setProperty(code, value);
		} catch(Exception e) {
			throw new KettleException("Error saving job entry attribute ["+code+"] for job entry with id ["+jobEntryId+"]", e);
		}
	}

	public void saveJobEntryAttribute(ObjectId jobId, ObjectId jobEntryId, int nr, String code, String value) throws KettleException {
		try {
			session.getNodeByUUID(jobEntryId.getId()).setProperty(code+PROPERTY_CODE_NR_SEPARATOR+nr, value);
		} catch(Exception e) {
			throw new KettleException("Error saving job entry attribute ["+code+"] for job entry with id ["+jobEntryId+"]", e);
		}
	}

	public void saveJobEntryAttribute(ObjectId jobId, ObjectId jobEntryId, int nr, String code, boolean value) throws KettleException {
		try {
			session.getNodeByUUID(jobEntryId.getId()).setProperty(code+PROPERTY_CODE_NR_SEPARATOR+nr, value);
		} catch(Exception e) {
			throw new KettleException("Error saving job entry attribute ["+code+"] for job entry with id ["+jobEntryId+"]", e);
		}
	}

	public void saveJobEntryAttribute(ObjectId jobId, ObjectId jobEntryId, int nr, String code, long value) throws KettleException {
		try {
			session.getNodeByUUID(jobEntryId.getId()).setProperty(code+PROPERTY_CODE_NR_SEPARATOR+nr, value);
		} catch(Exception e) {
			throw new KettleException("Error saving job entry attribute ["+code+"] for job entry with id ["+jobEntryId+"]", e);
		}
	}
	
	public int countNrJobEntryAttributes(ObjectId jobEntryId, String code) throws KettleException {
		try {
			Node jobEntryNode = session.getNodeByUUID(jobEntryId.getId());
			PropertyIterator properties = jobEntryNode.getProperties();
			int nr = 0;
			while (properties.hasNext()) {
				Property property = properties.nextProperty();
				if (property.getName().equals(code) || property.getName().startsWith(code+PROPERTY_CODE_NR_SEPARATOR)) {
					nr++;
				}
			}
			return nr; 
		} catch (RepositoryException e) {
			throw new KettleException("Unable to count the nr of job entry attributes for job entry with ID ["+jobEntryId+"] and code ["+code+"]", e);
		}
	}
	
	public boolean getJobEntryAttributeBoolean(ObjectId jobEntryId, String code) throws KettleException { 
		try {
			return session.getNodeByUUID(jobEntryId.getId()).getProperty(code).getBoolean();
		} catch(PathNotFoundException e) {
			return false;
		} catch (RepositoryException e) {
			throw new KettleException("Unable to get job entry attribute for entry with id ["+jobEntryId+"] and code ["+code+"]", e);
		}
	}
	
	public boolean getJobEntryAttributeBoolean(ObjectId jobEntryId, int nr, String code) throws KettleException {
		try {
			return session.getNodeByUUID(jobEntryId.getId()).getProperty(code+PROPERTY_CODE_NR_SEPARATOR+nr).getBoolean();
		} catch(PathNotFoundException e) {
			return false;
		} catch (RepositoryException e) {
			throw new KettleException("Unable to get job entry attribute for entry with id ["+jobEntryId+"] and code ["+code+"], nr="+nr, e);
		}
	}
	
	public boolean getJobEntryAttributeBoolean(ObjectId jobEntryId, String code, boolean def) throws KettleException {
		try {
			return session.getNodeByUUID(jobEntryId.getId()).getProperty(code).getBoolean();
		} catch(PathNotFoundException e) {
			return def;
		} catch (RepositoryException e) {
			throw new KettleException("Unable to get step attribute for step with id ["+jobEntryId+"] and code ["+code+"]", e);
		}
	}
	
	public long getJobEntryAttributeInteger(ObjectId jobEntryId, String code) throws KettleException { 
		try {
			return session.getNodeByUUID(jobEntryId.getId()).getProperty(code).getLong();
		} catch(PathNotFoundException e) {
			return 0L;
		} catch (RepositoryException e) {
			throw new KettleException("Unable to get job entry attribute for entry with id ["+jobEntryId+"] and code ["+code+"]", e);
		}
	}

	public long getJobEntryAttributeInteger(ObjectId jobEntryId, int nr, String code) throws KettleException { 
		try {
			return session.getNodeByUUID(jobEntryId.getId()).getProperty(code+PROPERTY_CODE_NR_SEPARATOR+nr).getLong();
		} catch(PathNotFoundException e) {
			return 0L;
		} catch (RepositoryException e) {
			throw new KettleException("Unable to get job entry attribute for entry with id ["+jobEntryId+"] and code ["+code+"], nr="+nr, e);
		}

	}
	
	public String getJobEntryAttributeString(ObjectId jobEntryId, String code) throws KettleException { 
		try {
			return session.getNodeByUUID(jobEntryId.getId()).getProperty(code).getString();
		} catch(PathNotFoundException e) {
			return null;
		} catch (RepositoryException e) {
			throw new KettleException("Unable to get job entry attribute for entry with id ["+jobEntryId+"] and code ["+code+"]", e);
		}
	}
	
	public String getJobEntryAttributeString(ObjectId jobEntryId, int nr, String code) throws KettleException {
		try {
			return session.getNodeByUUID(jobEntryId.getId()).getProperty(code+PROPERTY_CODE_NR_SEPARATOR+nr).getString();
		} catch(PathNotFoundException e) {
			return null;
		} catch (RepositoryException e) {
			throw new KettleException("Unable to get job entry attribute for entry with id ["+jobEntryId+"] and code ["+code+"], nr="+nr, e);
		}
	}

	public Node getJobEntryAttributeNode(ObjectId jobEntryId, String code) throws KettleException { 
		try {
			return session.getNodeByUUID(jobEntryId.getId()).getProperty(code).getNode();
		} catch(PathNotFoundException e) {
			return null;
		} catch (RepositoryException e) {
			throw new KettleException("Unable to get job entry attribute for entry with id ["+jobEntryId+"] and code ["+code+"]", e);
		}
	}

	// STEP ATTRIBUTES
	



	public int countNrStepAttributes(ObjectId stepId, String code) throws KettleException { 
		try {
			Node stepNode = session.getNodeByUUID(stepId.getId());
			PropertyIterator properties = stepNode.getProperties();
			int nr = 0;
			while (properties.hasNext()) {
				Property property = properties.nextProperty();
				if (property.getName().equals(code) || property.getName().startsWith(code+PROPERTY_CODE_NR_SEPARATOR)) {
					nr++;
				}
			}
			return nr; 
		} catch (RepositoryException e) {
			throw new KettleException("Unable to count the nr of step attributes for step with ID ["+stepId+"] and code ["+code+"]", e);
		}
	}

	public void saveStepAttribute(ObjectId transformationId, ObjectId stepId, String code, String value) throws KettleException {
		try {
			session.getNodeByUUID(stepId.getId()).setProperty(code, value);
		} catch(Exception e) {
			throw new KettleException("Error saving step attribute ["+code+"] for step with id ["+stepId+"]", e);
		}
	}

	/** Specially for JCR, creates a reference to a node
	 * 
	 * @param transformationId
	 * @param stepId
	 * @param code
	 * @param node The node to reference
	 * @throws KettleException
	 */
	public void saveStepAttribute(ObjectId transformationId, ObjectId stepId, String code, Node node) throws KettleException {
		try {
			session.getNodeByUUID(stepId.getId()).setProperty(code, node);
		} catch(Exception e) {
			throw new KettleException("Error saving step node reference attribute ["+code+"] for step with id ["+stepId+"]", e);
		}
	}

	public void saveStepAttribute(ObjectId transformationId, ObjectId stepId, String code, boolean value) throws KettleException {
		try {
			session.getNodeByUUID(stepId.getId()).setProperty(code, value);
		} catch(Exception e) {
			throw new KettleException("Error saving step attribute ["+code+"] for step with id ["+stepId+"]", e);
		}
	}

	public void saveStepAttribute(ObjectId transformationId, ObjectId stepId, String code, long value) throws KettleException {
		try {
			session.getNodeByUUID(stepId.getId()).setProperty(code, value);
		} catch(Exception e) {
			throw new KettleException("Error saving step attribute ["+code+"] for step with id ["+stepId+"]", e);
		}
	}

	public void saveStepAttribute(ObjectId transformationId, ObjectId stepId, String code, double value) throws KettleException {
		try {
			session.getNodeByUUID(stepId.getId()).setProperty(code, value);
		} catch(Exception e) {
			throw new KettleException("Error saving step attribute ["+code+"] for step with id ["+stepId+"]", e);
		}
	}

	public void saveStepAttribute(ObjectId transformationId, ObjectId stepId, int nr, String code, String value) throws KettleException {
		try {
			session.getNodeByUUID(stepId.getId()).setProperty(code+PROPERTY_CODE_NR_SEPARATOR+nr, value);
		} catch(Exception e) {
			throw new KettleException("Error saving step attribute ["+code+"] for step with id ["+stepId+"]", e);
		}
	}

	public void saveStepAttribute(ObjectId transformationId, ObjectId stepId, int nr, String code, boolean value) throws KettleException {
		try {
			session.getNodeByUUID(stepId.getId()).setProperty(code+PROPERTY_CODE_NR_SEPARATOR+nr, value);
		} catch(Exception e) {
			throw new KettleException("Error saving step attribute ["+code+"] for step with id ["+stepId+"]", e);
		}
	}

	public void saveStepAttribute(ObjectId transformationId, ObjectId stepId, int nr, String code, long value) throws KettleException {
		try {
			session.getNodeByUUID(stepId.getId()).setProperty(code+PROPERTY_CODE_NR_SEPARATOR+nr, value);
		} catch(Exception e) {
			throw new KettleException("Error saving step attribute ["+code+"] for step with id ["+stepId+"]", e);
		}
	}

	public void saveStepAttribute(ObjectId transformationId, ObjectId stepId, int nr, String code, double value) throws KettleException {
		try {
			session.getNodeByUUID(stepId.getId()).setProperty(code+PROPERTY_CODE_NR_SEPARATOR+nr, value);
		} catch(Exception e) {
			throw new KettleException("Error saving step attribute ["+code+"] for step with id ["+stepId+"]", e);
		}
	}

	public boolean getStepAttributeBoolean(ObjectId stepId, String code) throws KettleException { 
		try {
			return session.getNodeByUUID(stepId.getId()).getProperty(code).getBoolean();
		} catch(PathNotFoundException e) {
			return false;
		} catch (RepositoryException e) {
			throw new KettleException("Unable to get step attribute for step with id ["+stepId+"] and code ["+code+"]", e);
		}
	}
	
	public boolean getStepAttributeBoolean(ObjectId stepId, int nr, String code) throws KettleException { 
		try {
			return session.getNodeByUUID(stepId.getId()).getProperty(code+PROPERTY_CODE_NR_SEPARATOR+nr).getBoolean();
		} catch(PathNotFoundException e) {
			return false;
		} catch (RepositoryException e) {
			throw new KettleException("Unable to get step attribute for step with id ["+stepId+"] and code ["+code+"], nr="+nr, e);
		}
	}
	
	public boolean getStepAttributeBoolean(ObjectId stepId, int nr, String code, boolean def) throws KettleException { 
		try {
			return session.getNodeByUUID(stepId.getId()).getProperty(code+PROPERTY_CODE_NR_SEPARATOR+nr).getBoolean();
		} catch(PathNotFoundException e) {
			return def;
		} catch (RepositoryException e) {
			throw new KettleException("Unable to get step attribute for step with id ["+stepId+"] and code ["+code+"], nr="+nr, e);
		}
	}
	
	public long getStepAttributeInteger(ObjectId stepId, String code) throws KettleException { 
		try {
			return session.getNodeByUUID(stepId.getId()).getProperty(code).getLong();
		} catch(PathNotFoundException e) {
			return 0L;
		} catch (RepositoryException e) {
			throw new KettleException("Unable to get step attribute for step with id ["+stepId+"] and code ["+code+"]", e);
		}
	}
	
	public long getStepAttributeInteger(ObjectId stepId, int nr, String code) throws KettleException { 
		try {
			return session.getNodeByUUID(stepId.getId()).getProperty(code+PROPERTY_CODE_NR_SEPARATOR+nr).getLong();
		} catch(PathNotFoundException e) {
			return 0L;
		} catch (RepositoryException e) {
			throw new KettleException("Unable to get step attribute for step with id ["+stepId+"] and code ["+code+"], nr="+nr, e);
		}
	}
	
	public String getStepAttributeString(ObjectId stepId, String code) throws KettleException { 
		try {
			return session.getNodeByUUID(stepId.getId()).getProperty(code).getString();
		} catch(PathNotFoundException e) {
			return null;
		} catch (RepositoryException e) {
			throw new KettleException("Unable to get step attribute for step with id ["+stepId+"] and code ["+code+"]", e);
		}
	}
	public String getStepAttributeString(ObjectId stepId, int nr, String code) throws KettleException { 
		try {
			return session.getNodeByUUID(stepId.getId()).getProperty(code+PROPERTY_CODE_NR_SEPARATOR+nr).getString();
		} catch(PathNotFoundException e) {
			return null;
		} catch(RepositoryException e) {
			throw new KettleException("Unable to get step attribute for step with id ["+stepId+"] and code ["+code+"], nr="+nr, e);
		}
	}
	public Node getStepAttributeNode(ObjectId stepId, String code) throws KettleException { 
		try {
			return session.getNodeByUUID(stepId.getId()).getProperty(code).getNode();
		} catch(PathNotFoundException e) {
			return null;
		} catch (RepositoryException e) {
			throw new KettleException("Unable to get step attribute for step with id ["+stepId+"] and code ["+code+"]", e);
		}
	}

	
	
	
	
	public String getPropertyString(Node node, String code) throws KettleException {
		try {
			return node.getProperty(code).getString();
		} catch (PathNotFoundException e) {
			return null; // property is not defined
		} catch (RepositoryException e) {
			throw new KettleException("It was not possible to get information from property ["+code+"] in node", e);
		}
	}
	
	public long getPropertyLong(Node node, String code) throws KettleException {
		try {
			return node.getProperty(code).getLong();
		} catch (PathNotFoundException e) {
			return 0L; // property is not defined
		} catch (RepositoryException e) {
			throw new KettleException("It was not possible to get information from property ["+code+"] in node", e);
		}
	}

	public double getPropertyNumber(Node node, String code) throws KettleException {
		try {
			return node.getProperty(code).getDouble();
		} catch (PathNotFoundException e) {
			return 0.0; // property is not defined
		} catch (RepositoryException e) {
			throw new KettleException("It was not possible to get information from property ["+code+"] in node", e);
		}
	}

	public Date getPropertyDate(Node node, String code) throws KettleException {
		try {
			Calendar calendar = node.getProperty(code).getDate();
			return calendar.getTime();
		} catch (PathNotFoundException e) {
			return null; // property is not defined
		} catch (RepositoryException e) {
			throw new KettleException("It was not possible to get information from property ["+code+"] in node", e);
		}
	}
	public boolean getPropertyBoolean(Node node, String code) throws KettleException {
		try {
			return node.getProperty(code).getBoolean();
		} catch (PathNotFoundException e) {
			return false; // property is not defined
		} catch (RepositoryException e) {
			throw new KettleException("It was not possible to get information from property ["+code+"] in node", e);
		}
	}

	public boolean getPropertyBoolean(Node node, String code, boolean def) throws KettleException {
		try {
			return node.getProperty(code).getBoolean();
		} catch (PathNotFoundException e) {
			return def; // property is not defined, return the default
		} catch (RepositoryException e) {
			throw new KettleException("It was not possible to get information from property ["+code+"] in node", e);
		}
	}

	public Node getPropertyNode(Node node, String code) throws KettleException {
		try {
			return node.getProperty(code).getNode();
		} catch (PathNotFoundException e) {
			return null; // property is not defined
		} catch (RepositoryException e) {
			throw new KettleException("It was not possible to get information from property ["+code+"] in node", e);
		}
	}
	
	
	
	
	
	public void unlockJob(ObjectId jobId) throws KettleException {
		try {
			Node node = session.getNodeByUUID(jobId.getId());
			unlockNode(node); 
		} catch(Exception e) {
			throw new KettleException("Unable to unlock job with id ["+jobId+"]", e);
		}
	}

	/**
	 * @return the jcrRepositoryMeta
	 */
	public JCRRepositoryMeta getJcrRepositoryMeta() {
		return jcrRepositoryMeta;
	}

	/**
	 * @param jcrRepositoryMeta the jcrRepositoryMeta to set
	 */
	public void setJcrRepositoryMeta(JCRRepositoryMeta jcrRepositoryMeta) {
		this.jcrRepositoryMeta = jcrRepositoryMeta;
	}

	/**
	 * @return the jcrRepository
	 */
	public URLRemoteRepository getJcrRepository() {
		return jcrRepository;
	}

	/**
	 * @param jcrRepository the jcrRepository to set
	 */
	public void setJcrRepository(URLRemoteRepository jcrRepository) {
		this.jcrRepository = jcrRepository;
	}

	/**
	 * @param userInfo the userInfo to set
	 */
	public void setUserInfo(UserInfo userInfo) {
		this.userInfo = userInfo;
	}

	/**
	 * @return the workspace
	 */
	public Workspace getWorkspace() {
		return workspace;
	}

	/**
	 * @param workspace the workspace to set
	 */
	public void setWorkspace(Workspace workspace) {
		this.workspace = workspace;
	}

	/**
	 * @return the nodeTypeManager
	 */
	public NodeTypeManager getNodeTypeManager() {
		return nodeTypeManager;
	}

	/**
	 * @param nodeTypeManager the nodeTypeManager to set
	 */
	public void setNodeTypeManager(JackrabbitNodeTypeManager nodeTypeManager) {
		this.nodeTypeManager = nodeTypeManager;
	}

	/**
	 * @return the session
	 */
	public Session getSession() {
		return session;
	}
	
	public List<ObjectRevision> getRevisions(RepositoryElementLocationInterface element) throws KettleException {
		try {
			List<ObjectRevision> list = new ArrayList<ObjectRevision>();
			
			ObjectId objectId = getObjectId(element);
			if (objectId==null) {
				throw new KettleException("Unable to find repository element ["+element+"]");
			}
					
			Node node = session.getNodeByUUID(objectId.getId());
			VersionHistory versionHistory = node.getVersionHistory();
			Version version = versionHistory.getRootVersion();
			Version[] successors = version.getSuccessors();
			while (successors!=null && successors.length>0) {
				version = successors[0];
				successors = version.getSuccessors();
				list.add( getObjectRevision(version) );
			}
			
			return list;
		} catch(Exception e) {
			throw new KettleException("Could not retrieve version history of object with id ["+element+"]",e );
		}
	}
	
	public void undeleteObject(RepositoryElementLocationInterface element) throws KettleException {
		try {
			
			Node node = getNode(element.getName(), element.getRepositoryDirectory(), element.getRepositoryElementType().getExtension());

			boolean deleted = getPropertyBoolean(node, PROPERTY_DELETED, false);
			if (deleted) {
				// Undelete the last version, this again creates a new version.
				// We keep track of deletions this way.
				//
				node.checkout();
				node.setProperty(PROPERTY_DELETED, false);
				node.setProperty(PROPERTY_VERSION_COMMENT, "Marked "+element.getRepositoryElementType()+" as no longer deleted");
				node.save();
				node.checkin();
			}
			
		} catch(Exception e) {
			throw new KettleException("There was an error un-deleting repository element ["+element+"]", e);
		}
	}

	
	public ObjectRevision getObjectRevision(Version version) throws KettleException {
		try {
			Node versionNode = getVersionNode(version);
			
			String comment = getPropertyString(versionNode, PROPERTY_VERSION_COMMENT);
			String userCreated = getPropertyString(versionNode, PROPERTY_USER_CREATED);
	
	
			return new JCRObjectRevision(version, comment, userCreated);
		} catch(Exception e) {
			throw new KettleException("Unable to retrieve revision information from an object version", e);
		}
	}
	
	public Version getVersion(Node node, String versionLabel) throws KettleException {
		try {
			if (Const.isEmpty(versionLabel)) {
				return getLastVersion(node);
			} else {
				return node.getVersionHistory().getVersion(versionLabel);
			}
		} catch(VersionException e) {
			throw new KettleException("Unable to find version ["+versionLabel+"]", e);
		} catch(Exception e) {
			throw new KettleException("Error getting version ["+versionLabel+"]", e);
		}
	}

	/**
	 * An object is never deleted, simply marked as such!
	 * 
	 * @param id 
	 * @throws KettleException
	 */
	public void deleteObject(ObjectId id, RepositoryObjectType objectType) throws KettleException {
		try {
			// What is the main object node?
			//
			Node node;
			
			try {
				node = session.getNodeByUUID(id.getId());
			} catch(ItemNotFoundException e) {
				// It's already gone!
				return;
			}
			
			deleteObject(node, objectType);
		}
		catch(Exception e) {
			throw new KettleException("Unable to mark object with ID ["+id+"] as deleted", e);
		}
	}
		

	/**
	 * An node is never deleted, simply marked as such!
	 * This is because of versioning reasons!
	 * 
	 * @param node
	 * @throws KettleException
	 */
	public void deleteObject(Node node, RepositoryObjectType objectType) throws KettleException {
		try {
			node.checkout();
			node.setProperty(PROPERTY_DELETED, true);
			node.setProperty(PROPERTY_VERSION_COMMENT, "Marked "+objectType+" as deleted");
			session.save();
			node.checkin();
		} catch(Exception e) {
			throw new KettleException("Unable to mark object as deleted", e);
		}
	}

	/**
	 * @return the rootNode
	 */
	public Node getRootNode() {
		return rootNode;
	}

	/**
	 * Returns a name. It is always required.
	 * @param node the node to get the name from
	 * @return The name of the node
	 * @throws Exception If no name property is found, an exception will be throws (PathNotFoundException)
	 */
	public String getObjectName(Node node) throws Exception {
		return node.getProperty(JCRRepository.PROPERTY_NAME).getString();
	}

	/**
	 * Returns the description of the node.  If no description is found, it returns null.
	 * @param node the node to get the description from
	 * @return the description of the node or null.
	 * @throws Exception In case something goes wrong (I/O, network, etc)
	 */
	public String getObjectDescription(Node node) throws Exception {
		return getPropertyString(node, JCRRepository.PROPERTY_DESCRIPTION);
	}

	public Node createOrVersionNode(RepositoryElementInterface element, String versionComment) throws Exception {
		String ext = element.getRepositoryElementType().getExtension();
		String name = sanitizeNodeName(element.getName())+ext;
		Node folder = findFolderNode(element.getRepositoryDirectory());
		
		// First see if a node with the same name already exists...
		//
		Node node = null;
		RepositoryLock lock = null;
		
		ObjectId id = getObjectId(element);
		if (id!=null) {
			node = session.getNodeByUUID(id.getId());
			
			lock = getLock(id);
			
			if (lock!=null) {
				if (!getUserInfo().getLogin().equals(lock.getLogin())) {
					throw new KettleException("This object is locked by user ["+lock.getLogin()+"] @ "+lock.getLockDate()+" with message ["+lock.getMessage()+"], it needs to be unlocked before changes can be made.");
				} else {
					unlockNode(node);
				}
			}

			// We need to perform a check out to generate a new version
			// 
			node.checkout();
			
			// Remove all existing properties, they have to be provided for again
			//
			PropertyIterator properties = node.getProperties();
			while (properties.hasNext()) {
				Property property = properties.nextProperty();
				if (!property.getName().startsWith("jcr:")) {
					node.setProperty(property.getName(), (String)null);
				}
			}
			
			// Remove all sub-nodes, they have to be provided for again
			//
			NodeIterator nodes = node.getNodes();
			while (nodes.hasNext()) {
				Node child = nodes.nextNode();
				child.remove();
			}
		} else {
			// Create a new node
			//
			node = folder.addNode(name, JCRRepository.NODE_TYPE_UNSTRUCTURED);
			node.addMixin(JCRRepository.MIX_VERSIONABLE);
			node.addMixin(JCRRepository.MIX_REFERENCEABLE);
			
			node.checkout();
		}
		
		node.setProperty(PROPERTY_DELETED, false);
		node.setProperty(PROPERTY_NAME, element.getName());
		node.setProperty(PROPERTY_DESCRIPTION, element.getDescription());
		node.setProperty(PROPERTY_VERSION_COMMENT, versionComment);
		node.setProperty(PROPERTY_USER_CREATED, userInfo.getLogin());

		return node;
	}
	
	/**
	 * Sanitize the name of an object so we can use it as a node name...
	 * Usually this means we can include any colons in it. 
	 * 
	 * @param name The name to clean up
	 * @return the sanitized name
	 */
	public String sanitizeNodeName(String name) {
		StringBuffer result = new StringBuffer(30);
		
		for (char c : name.toCharArray()) {
			switch(c) {
			case ':' : result.append('-'); break;
			case '/' : result.append("-"); break;
			default : result.append(c); break;
			}
		}
		
		return result.toString();
	}
}