package org.pentaho.di.repository.pur;

import java.util.List;

import org.pentaho.di.cluster.ClusterSchema;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.ProgressMonitorListener;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleSecurityException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.partition.PartitionSchema;
import org.pentaho.di.repository.ObjectAcl;
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
import org.pentaho.platform.repository.pcr.data.node.DataNode;
import org.pentaho.platform.repository.pcr.data.node.DataNodeRef;
import org.pentaho.platform.repository.pcr.data.node.DataProperty;

/**
 * A {@link Repository} that stands in for the real repository, collecting entry and step attributes and loading or
 * saving them as a batch. Use one instance of this class per entry or step!
 */
public class RepositoryProxy implements Repository {

  public static final String PROP_CODE_NR_SEPARATOR = "_#_"; //$NON-NLS-1$

  private static final String PROPERTY_XML = "XML"; //$NON-NLS-1$

  private DataNode node;

  public RepositoryProxy(final DataNode node) {
    super();
    this.node = node;
  }

  public void connect() throws KettleException, KettleSecurityException {
    throw new UnsupportedOperationException();
  }

  public int countNrJobEntryAttributes(ObjectId idJobentry, String code) throws KettleException {
    return getPropertyCount(idJobentry, code);
  }

  protected int getPropertyCount(final ObjectId idStep, final String code) throws KettleException {
    int count = 0;
    for (DataProperty prop : node.getProperties()) {
      if (prop.getName().startsWith(code + PROP_CODE_NR_SEPARATOR)) {
        count++;
      }
    }
    return count;
  }

  public int countNrStepAttributes(final ObjectId idStep, final String code) throws KettleException {
    return getPropertyCount(idStep, code);
  }

  public RepositoryDirectory createRepositoryDirectory(final RepositoryDirectory parentDirectory,
      final String directoryPath) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void deleteClusterSchema(ObjectId idCluster) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void deleteDatabaseMeta(String databaseName) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void deleteJob(ObjectId idJob) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void deletePartitionSchema(ObjectId idPartitionSchema) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void deleteRepositoryDirectory(RepositoryDirectory dir) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void deleteSlave(ObjectId idSlave) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void deleteTransformation(ObjectId idTransformation) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void disconnect() {
    throw new UnsupportedOperationException();
  }

  public boolean exists(String name, RepositoryDirectory repositoryDirectory, RepositoryObjectType objectType)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public ObjectId getClusterID(String name) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public ObjectId[] getClusterIDs(boolean includeDeleted) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public String[] getClusterNames(boolean includeDeleted) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public ObjectId getDatabaseID(String name) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public ObjectId[] getDatabaseIDs(boolean includeDeleted) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public String[] getDatabaseNames(boolean includeDeleted) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public String[] getDirectoryNames(ObjectId idDirectory) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public boolean getJobEntryAttributeBoolean(ObjectId idJobentry, String code) throws KettleException {
    return node.getProperty(code).getBoolean();
  }

  public boolean getJobEntryAttributeBoolean(ObjectId idJobentry, int nr, String code) throws KettleException {
    return node.getProperty(code + PROP_CODE_NR_SEPARATOR + nr).getBoolean();
  }

  public boolean getJobEntryAttributeBoolean(ObjectId idJobentry, String code, boolean def) throws KettleException {
    if (node.hasProperty(code)) {
      return node.getProperty(code).getBoolean();
    } else {
      return def;
    }
  }

  public long getJobEntryAttributeInteger(ObjectId idJobentry, String code) throws KettleException {
    return node.getProperty(code).getLong();
  }

  public long getJobEntryAttributeInteger(ObjectId idJobentry, int nr, String code) throws KettleException {
    return node.getProperty(code + PROP_CODE_NR_SEPARATOR + nr).getLong();
  }

  public String getJobEntryAttributeString(ObjectId idJobentry, String code) throws KettleException {
    if (node.hasProperty(code)) {
      return node.getProperty(code).getString();
    } else {
      return null;
    }
  }

  public String getJobEntryAttributeString(ObjectId idJobentry, int nr, String code) throws KettleException {
    String propName = code + PROP_CODE_NR_SEPARATOR + nr;
    if (node.hasProperty(propName)) {
      return node.getProperty(propName).getString();
    } else {
      return null;
    }
  }

  public ObjectId getJobId(String name, RepositoryDirectory repositoryDirectory) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public RepositoryLock getJobLock(ObjectId idJob) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public String[] getJobNames(ObjectId idDirectory, boolean includeDeleted) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public List<RepositoryObject> getJobObjects(ObjectId idDirectory, boolean includeDeleted) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public LogChannelInterface getLog() {
    throw new UnsupportedOperationException();
  }

  public String getName() {
    throw new UnsupportedOperationException();
  }

  public ObjectId getPartitionSchemaID(String name) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public ObjectId[] getPartitionSchemaIDs(boolean includeDeleted) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public String[] getPartitionSchemaNames(boolean includeDeleted) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public RepositoryMeta getRepositoryMeta() {
    throw new UnsupportedOperationException();
  }

  public List<ObjectRevision> getRevisions(RepositoryElementLocationInterface element) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public RepositorySecurityProvider getSecurityProvider() {
    throw new UnsupportedOperationException();
  }

  public ObjectId getSlaveID(String name) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public ObjectId[] getSlaveIDs(boolean includeDeleted) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public String[] getSlaveNames(boolean includeDeleted) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public List<SlaveServer> getSlaveServers() throws KettleException {
    throw new UnsupportedOperationException();
  }

  public boolean getStepAttributeBoolean(ObjectId idStep, String code) throws KettleException {
    return node.getProperty(code).getBoolean();
  }

  public boolean getStepAttributeBoolean(ObjectId idStep, int nr, String code) throws KettleException {
    return node.getProperty(code + PROP_CODE_NR_SEPARATOR + nr).getBoolean();
  }

  public boolean getStepAttributeBoolean(ObjectId idStep, int nr, String code, boolean def) throws KettleException {
    if (node.hasProperty(code + PROP_CODE_NR_SEPARATOR + nr)) {
      return node.getProperty(code + PROP_CODE_NR_SEPARATOR + nr).getBoolean();
    } else {
      return def;
    }
  }

  public long getStepAttributeInteger(ObjectId idStep, String code) throws KettleException {
    return node.getProperty(code).getLong();
  }

  public long getStepAttributeInteger(ObjectId idStep, int nr, String code) throws KettleException {
    return node.getProperty(code + PROP_CODE_NR_SEPARATOR + nr).getLong();
  }

  public String getStepAttributeString(ObjectId idStep, String code) throws KettleException {
    if (node.hasProperty(code)) {
      return node.getProperty(code).getString();
    } else {
      return null;
    }
  }

  public String getStepAttributeString(ObjectId idStep, int nr, String code) throws KettleException {
    String propName = code + PROP_CODE_NR_SEPARATOR + nr;
    if (node.hasProperty(propName)) {
      return node.getProperty(propName).getString();
    } else {
      return null;
    }
  }

  public ObjectId getTransformationID(String name, RepositoryDirectory repositoryDirectory) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public RepositoryLock getTransformationLock(ObjectId idTransformation) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public String[] getTransformationNames(ObjectId idDirectory, boolean includeDeleted) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public List<RepositoryObject> getTransformationObjects(ObjectId idDirectory, boolean includeDeleted)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public UserInfo getUserInfo() {
    throw new UnsupportedOperationException();
  }

  public String getVersion() {
    throw new UnsupportedOperationException();
  }

  public RepositoryVersionRegistry getVersionRegistry() throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void init(RepositoryMeta repositoryMeta, UserInfo userInfo) {
    throw new UnsupportedOperationException();
  }

  public void insertJobEntryDatabase(ObjectId idJob, ObjectId idJobentry, ObjectId idDatabase) throws KettleException {
    DataNodeRef ref = new DataNodeRef(idDatabase.getId());
    node.setProperty(idDatabase.getId(), ref);
  }

  public ObjectId insertLogEntry(String description) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void insertStepDatabase(ObjectId idTransformation, ObjectId idStep, ObjectId idDatabase)
      throws KettleException {
    DataNodeRef ref = new DataNodeRef(idDatabase.getId());
    node.setProperty(idDatabase.getId(), ref);
  }

  public boolean isConnected() {
    throw new UnsupportedOperationException();
  }

  public ClusterSchema loadClusterSchema(ObjectId idClusterSchema, List<SlaveServer> slaveServers, String versionLabel)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public Condition loadConditionFromStepAttribute(ObjectId idStep, String code) throws KettleException {
    DataNode conditionNode = node.getNode(code);
    if (conditionNode.hasProperty(PROPERTY_XML)) {
      String xml = conditionNode.getProperty(PROPERTY_XML).getString();
      Condition condition = new Condition(XMLHandler.getSubNode(XMLHandler.loadXMLString(xml), Condition.XML_TAG));
      return condition;
    } else {
      return null;
    }
  }

  public DatabaseMeta loadDatabaseMeta(ObjectId idDatabase, String revision) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public DatabaseMeta loadDatabaseMetaFromJobEntryAttribute(ObjectId idJobentry, String nameCode, String code,
      List<DatabaseMeta> databases) throws KettleException {
    if (code != null && node.hasProperty(code)) {
      ObjectId databaseId = new StringObjectId(node.getProperty(code).getRef().getId().toString());
      return DatabaseMeta.findDatabase(databases, databaseId);
    }
    return null;
  }

  public DatabaseMeta loadDatabaseMetaFromStepAttribute(ObjectId idStep, String code, List<DatabaseMeta> databases)
      throws KettleException {
    if (code != null && node.hasProperty(code)) {
      ObjectId databaseId = new StringObjectId(node.getProperty(code).getRef().getId().toString());
      return DatabaseMeta.findDatabase(databases, databaseId);
    }
    return null;
  }

  public JobMeta loadJob(String jobname, RepositoryDirectory repdir, ProgressMonitorListener monitor, String revision)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public PartitionSchema loadPartitionSchema(ObjectId idPartitionSchema, String versionLabel) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public RepositoryDirectory loadRepositoryDirectoryTree() throws KettleException {
    throw new UnsupportedOperationException();
  }

  public SlaveServer loadSlaveServer(ObjectId idSlaveServer, String versionLabel) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public TransMeta loadTransformation(String transname, RepositoryDirectory repdir, ProgressMonitorListener monitor,
      boolean setInternalVariables, String revision) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void lockJob(ObjectId idJob, String message) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void lockTransformation(ObjectId idTransformation, String message) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public List<DatabaseMeta> readDatabases() throws KettleException {
    throw new UnsupportedOperationException();
  }

  public SharedObjects readJobMetaSharedObjects(JobMeta jobMeta) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public SharedObjects readTransSharedObjects(TransMeta transMeta) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public ObjectId renameDatabase(ObjectId idDatabase, String newname) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public ObjectId renameJob(ObjectId idJob, RepositoryDirectory newDirectory, String newName) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public ObjectId renameRepositoryDirectory(RepositoryDirectory dir) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public ObjectId renameRepositoryDirectory(ObjectId id, RepositoryDirectory newParentDir, String newName)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public ObjectId renameTransformation(ObjectId idTransformation, RepositoryDirectory newDirectory, String newName)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void save(RepositoryElementInterface repositoryElement, String versionComment, ProgressMonitorListener monitor)
      throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void saveConditionStepAttribute(ObjectId idTransformation, ObjectId idStep, String code, Condition condition)
      throws KettleException {
    DataNode conditionNode = node.addNode(code);
    conditionNode.setProperty(PROPERTY_XML, condition.getXML());
  }

  public void saveDatabaseMetaJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, String nameCode, String code,
      DatabaseMeta database) throws KettleException {
    if (database != null && database.getObjectId() != null) {
      DataNodeRef ref = new DataNodeRef(database.getObjectId().getId());
      node.setProperty(code, ref);
    }
  }

  public void saveDatabaseMetaStepAttribute(ObjectId idTransformation, ObjectId idStep, String code,
      DatabaseMeta database) throws KettleException {
    if (database != null && database.getObjectId() != null) {
      DataNodeRef ref = new DataNodeRef(database.getObjectId().getId());
      node.setProperty(code, ref);
    }
  }

  public void saveJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, String code, String value)
      throws KettleException {
    node.setProperty(code, value);
  }

  public void saveJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, String code, boolean value)
      throws KettleException {
    node.setProperty(code, value);
  }

  public void saveJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, String code, long value)
      throws KettleException {
    node.setProperty(code, value);
  }

  public void saveJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, int nr, String code, String value)
      throws KettleException {
    node.setProperty(code + PROP_CODE_NR_SEPARATOR + nr, value);
  }

  public void saveJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, int nr, String code, boolean value)
      throws KettleException {
    node.setProperty(code + PROP_CODE_NR_SEPARATOR + nr, value);
  }

  public void saveJobEntryAttribute(ObjectId idJob, ObjectId idJobentry, int nr, String code, long value)
      throws KettleException {
    node.setProperty(code + PROP_CODE_NR_SEPARATOR + nr, value);
  }

  public void saveRepositoryDirectory(RepositoryDirectory dir) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, String code, String value)
      throws KettleException {
    node.setProperty(code, value);
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, String code, boolean value)
      throws KettleException {
    node.setProperty(code, value);
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, String code, long value)
      throws KettleException {
    node.setProperty(code, value);
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, String code, double value)
      throws KettleException {
    node.setProperty(code, value);
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, int nr, String code, String value)
      throws KettleException {
    node.setProperty(code + PROP_CODE_NR_SEPARATOR + nr, value);
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, int nr, String code, boolean value)
      throws KettleException {
    node.setProperty(code + PROP_CODE_NR_SEPARATOR + nr, value);
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, int nr, String code, long value)
      throws KettleException {
    node.setProperty(code + PROP_CODE_NR_SEPARATOR + nr, value);
  }

  public void saveStepAttribute(ObjectId idTransformation, ObjectId idStep, int nr, String code, double value)
      throws KettleException {
    node.setProperty(code + PROP_CODE_NR_SEPARATOR + nr, value);
  }

  public void undeleteObject(RepositoryElementLocationInterface element) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void unlockJob(ObjectId idJob) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void unlockTransformation(ObjectId idTransformation) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public ObjectAcl getAcl(ObjectId arg0) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public List<ObjectRevision> getRevisions(ObjectId arg0) throws KettleException {
    throw new UnsupportedOperationException();
  }

  public void setAcl(ObjectId arg0, ObjectAcl arg1) throws KettleException {
    throw new UnsupportedOperationException();
  }

}
