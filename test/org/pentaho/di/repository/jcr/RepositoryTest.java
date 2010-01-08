package org.pentaho.di.repository.jcr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jackrabbit.core.TransientRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.pentaho.di.cluster.ClusterSchema;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.NotePadMeta;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.annotations.Job;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.ChannelLogTable;
import org.pentaho.di.core.logging.JobEntryLogTable;
import org.pentaho.di.core.logging.JobLogTable;
import org.pentaho.di.core.logging.PerformanceLogTable;
import org.pentaho.di.core.logging.StepLogTable;
import org.pentaho.di.core.logging.TransLogTable;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.job.JobHopMeta;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.partition.PartitionSchema;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.ObjectRevision;
import org.pentaho.di.repository.ProfileMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryCapabilities;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.di.repository.ProfileMeta.Permission;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransDependency;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.tableexists.TableExistsMeta;
import org.w3c.dom.Node;

import com.pentaho.commons.dsc.PentahoLicenseVerifier;
import com.pentaho.commons.dsc.util.TestLicenseStream;

public class RepositoryTest {

  // ~ Static fields/initializers ======================================================================================

  private static final String VERSION_COMMENT_V1 = "hello";

  private static final String VERSION_LABEL_V1 = "1.0";

  private static final Log logger = LogFactory.getLog(RepositoryTest.class);

  private static final String DIR_CONNECTIONS = "connections";

  private static final String DIR_SCHEMAS = "schemas";

  private static final String DIR_SLAVES = "slaves";

  private static final String DIR_CLUSTERS = "clusters";

  private static final String DIR_TRANSFORMATIONS = "transformations";

  private static final String DIR_JOBS = "jobs";

  private static final String DIR_TMP = "tmp";

  private static final String EXP_JOB_NAME = "job1";

  private static final String EXP_JOB_DESC = "jobDesc";

  private static final String EXP_JOB_EXTENDED_DESC = "jobExtDesc";

  private static final String EXP_JOB_VERSION = "anything";

  private static final int EXP_JOB_STATUS = 12;

  private static final String EXP_JOB_CREATED_USER = "jerry";

  private static final Date EXP_JOB_CREATED_DATE = new Date();

  private static final String EXP_JOB_MOD_USER = "george";

  private static final Date EXP_JOB_MOD_DATE = new Date();

  private static final String EXP_JOB_PARAM_1_DESC = "param1desc";

  private static final String EXP_JOB_PARAM_1_NAME = "param1";

  private static final String EXP_JOB_PARAM_1_DEF = "param1default";

  private static final String EXP_JOB_LOG_TABLE_INTERVAL = "15";

  private static final String EXP_JOB_LOG_TABLE_CONN_NAME = "connName";

  private static final String EXP_JOB_LOG_TABLE_SCHEMA_NAME = "schemaName";

  private static final String EXP_JOB_LOG_TABLE_TABLE_NAME = "tableName";

  private static final String EXP_JOB_LOG_TABLE_TIMEOUT_IN_DAYS = "2";

  private static final String EXP_JOB_LOG_TABLE_SIZE_LIMIT = "250";

  private static final boolean EXP_JOB_BATCH_ID_PASSED = true;

  private static final String EXP_JOB_SHARED_OBJECTS_FILE = ".kettle/whatever";

  private static final String EXP_JOB_ENTRY_1_NAME = "createFile";

  private static final String EXP_JOB_ENTRY_1_FILENAME = "/tmp/whatever";

  private static final String EXP_JOB_ENTRY_2_NAME = "deleteFile";

  private static final String EXP_JOB_ENTRY_2_FILENAME = "/tmp/whatever";

  private static final int EXP_JOB_ENTRY_1_COPY_X_LOC = 10;

  private static final int EXP_JOB_ENTRY_1_COPY_Y_LOC = 10;

  private static final int EXP_JOB_ENTRY_2_COPY_X_LOC = 75;

  private static final int EXP_JOB_ENTRY_2_COPY_Y_LOC = 10;

  private static final int EXP_NOTEPAD_X = 10;

  private static final String EXP_NOTEPAD_NOTE = "blah";

  private static final int EXP_NOTEPAD_Y = 200;

  private static final int EXP_NOTEPAD_WIDTH = 50;

  private static final int EXP_NOTEPAD_HEIGHT = 25;

  private static final String EXP_DBMETA_NAME = "haha";

  private static final String EXP_DBMETA_HOSTNAME = "acme";

  private static final String EXP_DBMETA_TYPE = "ORACLE";

  private static final int EXP_DBMETA_ACCESS = DatabaseMeta.TYPE_ACCESS_NATIVE;

  private static final String EXP_DBMETA_DBNAME = "lksjdf";

  private static final String EXP_DBMETA_PORT = "10521";

  private static final String EXP_DBMETA_USERNAME = "elaine";

  private static final String EXP_DBMETA_PASSWORD = "password";

  private static final String EXP_DBMETA_SERVERNAME = "serverName";

  private static final String EXP_DBMETA_DATA_TABLESPACE = "dataTablespace";

  private static final String EXP_DBMETA_INDEX_TABLESPACE = "indexTablespace";

  private static final String EXP_SLAVE_NAME = "slave54545";

  private static final String EXP_SLAVE_HOSTNAME = "slave98745";

  private static final String EXP_SLAVE_PORT = "11111";

  private static final String EXP_SLAVE_USERNAME = "cosmo";

  private static final String EXP_SLAVE_PASSWORD = "password";

  private static final String EXP_SLAVE_PROXY_HOSTNAME = "proxySlave542254";

  private static final String EXP_SLAVE_PROXY_PORT = "11112";

  private static final String EXP_SLAVE_NON_PROXY_HOSTS = "ljksdflsdf";

  private static final boolean EXP_SLAVE_MASTER = true;

  private static final String EXP_SLAVE_HOSTNAME_V2 = "slave98561111";

  private static final String EXP_DBMETA_HOSTNAME_V2 = "acme98734";

  private static final String VERSION_COMMENT_V2 = "v2 blah blah blah";

  private static final String EXP_JOB_DESC_V2 = "jobDesc0368";

  private static final String EXP_TRANS_NAME = "transMeta";

  private static final String EXP_TRANS_DESC = "transMetaDesc";

  private static final String EXP_TRANS_EXTENDED_DESC = "transMetaExtDesc";

  private static final String EXP_TRANS_VERSION = "2.0";

  private static final int EXP_TRANS_STATUS = 2;

  private static final String EXP_TRANS_PARAM_1_DESC = "transParam1Desc";

  private static final String EXP_TRANS_PARAM_1_DEF = "transParam1Def";

  private static final String EXP_TRANS_PARAM_1_NAME = "transParamName";

  private static final String EXP_TRANS_CREATED_USER = "newman";

  private static final Date EXP_TRANS_CREATED_DATE = new Date();

  private static final String EXP_TRANS_MOD_USER = "banya";

  private static final Date EXP_TRANS_MOD_DATE = new Date();

  private static final String EXP_TRANS_LOG_TABLE_CONN_NAME = "transLogTableConnName";

  private static final String EXP_TRANS_LOG_TABLE_INTERVAL = "34";

  private static final String EXP_TRANS_LOG_TABLE_SCHEMA_NAME = "transLogTableSchemaName";

  private static final String EXP_TRANS_LOG_TABLE_SIZE_LIMIT = "600";

  private static final String EXP_TRANS_LOG_TABLE_TABLE_NAME = "transLogTableTableName";

  private static final String EXP_TRANS_LOG_TABLE_TIMEOUT_IN_DAYS = "5";

  private static final String EXP_TRANS_MAX_DATE_TABLE = "transMaxDateTable";

  private static final String EXP_TRANS_MAX_DATE_FIELD = "transMaxDateField";

  private static final double EXP_TRANS_MAX_DATE_OFFSET = 55;

  private static final double EXP_TRANS_MAX_DATE_DIFF = 70;

  private static final int EXP_TRANS_SIZE_ROWSET = 833;

  private static final int EXP_TRANS_SLEEP_TIME_EMPTY = 4;

  private static final int EXP_TRANS_SLEEP_TIME_FULL = 9;

  private static final boolean EXP_TRANS_USING_UNIQUE_CONN = true;

  private static final boolean EXP_TRANS_FEEDBACK_SHOWN = true;

  private static final int EXP_TRANS_FEEDBACK_SIZE = 222;

  private static final boolean EXP_TRANS_USING_THREAD_PRIORITY_MGMT = true;

  private static final String EXP_TRANS_SHARED_OBJECTS_FILE = "transSharedObjectsFile";

  private static final boolean EXP_TRANS_CAPTURE_STEP_PERF_SNAPSHOTS = true;

  private static final long EXP_TRANS_STEP_PERF_CAP_DELAY = 81;

  private static final String EXP_TRANS_DEP_TABLE_NAME = "KLKJSDF";

  private static final String EXP_TRANS_DEP_FIELD_NAME = "lkjsdfflll11";

  private static final String EXP_PART_SCHEMA_NAME = "partitionSchemaName";

  private static final String EXP_PART_SCHEMA_PARTID_2 = "partitionSchemaId2";

  private static final boolean EXP_PART_SCHEMA_DYN_DEF = true;

  private static final String EXP_PART_SCHEMA_PART_PER_SLAVE_COUNT = "562";

  private static final String EXP_PART_SCHEMA_PARTID_1 = "partitionSchemaId1";

  private static final String EXP_PART_SCHEMA_DESC = "partitionSchemaDesc";

  private static final String EXP_PART_SCHEMA_PART_PER_SLAVE_COUNT_V2 = "563";

  private static final String EXP_CLUSTER_SCHEMA_NAME = "clusterSchemaName";

  private static final String EXP_CLUSTER_SCHEMA_SOCKETS_BUFFER_SIZE = "2048";

  private static final String EXP_CLUSTER_SCHEMA_BASE_PORT = "12456";

  private static final String EXP_CLUSTER_SCHEMA_SOCKETS_FLUSH_INTERVAL = "1500";

  private static final boolean EXP_CLUSTER_SCHEMA_SOCKETS_COMPRESSED = true;

  private static final boolean EXP_CLUSTER_SCHEMA_DYN = true;

  private static final String EXP_CLUSTER_SCHEMA_BASE_PORT_V2 = "12457";

  private static final String EXP_TRANS_STEP_1_NAME = "transStep1";

  private static final String EXP_TRANS_STEP_2_NAME = "transStep2";

  // ~ Instance fields =================================================================================================

  protected RepositoryMeta repositoryMeta;

  protected Repository repository;

  protected UserInfo userInfo;

  // ~ Constructors ====================================================================================================

  // ~ Methods =========================================================================================================

  @Before
  public void setUp() throws Exception {
    // tell kettle to look for plugins in this package (because custom plugins are defined in this class)
    System.setProperty(Const.KETTLE_PLUGIN_PACKAGES, this.getClass().getPackage().getName());

    PentahoLicenseVerifier.setStreamOpener(new TestLicenseStream("pdi-ee=true")); //$NON-NLS-1$
    KettleEnvironment.init();
    repositoryMeta = new JCRRepositoryMeta();
    repositoryMeta.setName("JackRabbit");
    repositoryMeta.setDescription("JackRabbit test repository");
    ((JCRRepositoryMeta) repositoryMeta).setRepositoryLocation(new JCRRepositoryLocation(
        "http://localhost:8080/jackrabbit/rmi"));
    ProfileMeta adminProfile = new ProfileMeta("admin", "Administrator");
    adminProfile.addPermission(Permission.ADMIN);
    userInfo = new UserInfo("joe", "password", "Apache Tomcat", "Apache Tomcat user", true, adminProfile);
    repository = new JCRRepository();
    File repoDir = new File("/tmp/pdi_jcr_repo_unit_test");
    assertTrue(repoDir.mkdir());
    javax.jcr.Repository jcrRepository = new TransientRepository(repoDir);
    ((JCRRepository) repository).setJcrRepository(jcrRepository);
    repository.init(repositoryMeta, userInfo);
    repository.connect();
  }

  @After
  public void tearDown() throws Exception {
    repository.disconnect();
    repositoryMeta = null;
    repository = null;
    userInfo = null;
    FileUtils.deleteDirectory(new File("/tmp/pdi_jcr_repo_unit_test"));
  }

  /**
   * getUserInfo()
   * getVersion()
   * getName()
   * isConnected()
   * getRepositoryMeta()
   * getLog()
   */
  @Test
  public void testVarious() throws Exception {
    UserInfo userInfo = repository.getUserInfo();
    // unfortunately UserInfo doesn't override equals()
    assertEquals("joe", userInfo.getName());
    assertEquals("password", userInfo.getPassword());
    assertEquals("Apache Tomcat", userInfo.getUsername());
    assertEquals("Apache Tomcat user", userInfo.getDescription());
    assertTrue(userInfo.isEnabled());
    assertEquals("admin", userInfo.getProfile().getName());
    assertEquals("Administrator", userInfo.getProfile().getDescription());
    assertEquals(Permission.ADMIN, userInfo.getProfile().getPermission(0));
    assertEquals(VERSION_LABEL_V1, repository.getVersion());
    assertEquals("JackRabbit", repository.getName());
    assertTrue(repository.isConnected());
    RepositoryMeta repoMeta = repository.getRepositoryMeta();
    assertEquals("JackRabbit", repoMeta.getName());
    assertEquals("JackRabbit test repository", repoMeta.getDescription());
    RepositoryCapabilities caps = repoMeta.getRepositoryCapabilities();
    assertTrue(caps.supportsUsers());
    assertFalse(caps.managesUsers());
    assertFalse(caps.isReadOnly());
    assertTrue(caps.supportsRevisions());
    assertTrue(caps.supportsMetadata());
    assertTrue(caps.supportsLocking());
    assertTrue(caps.hasVersionRegistry());
    assertNull(repository.getLog());
  }

  private RepositoryDirectory initRepo() throws Exception {
    RepositoryDirectory rootDir = repository.loadRepositoryDirectoryTree();
    repository.createRepositoryDirectory(rootDir, DIR_CONNECTIONS);
    repository.createRepositoryDirectory(rootDir, DIR_SCHEMAS);
    repository.createRepositoryDirectory(rootDir, DIR_SLAVES);
    repository.createRepositoryDirectory(rootDir, DIR_CLUSTERS);
    repository.createRepositoryDirectory(rootDir, DIR_TRANSFORMATIONS);
    repository.createRepositoryDirectory(rootDir, DIR_JOBS);
    return repository.loadRepositoryDirectoryTree();
  }

  /**
   * createRepositoryDirectory()
   * loadRepositoryTree()
   * deleteRepositoryDirectory()
   * getDirectoryNames()
   * saveRepositoryDirectory()
   */
  @Test
  public void testDirectories() throws Exception {
    RepositoryDirectory rootDir = repository.loadRepositoryDirectoryTree();
    RepositoryDirectory connDir = repository.createRepositoryDirectory(rootDir, DIR_CONNECTIONS);
    assertNotNull(connDir);
    assertNotNull(connDir.getObjectId());
    assertEquals(RepositoryDirectory.DIRECTORY_SEPARATOR + DIR_CONNECTIONS, connDir.getPath());
    repository.createRepositoryDirectory(rootDir, DIR_SCHEMAS);
    repository.createRepositoryDirectory(rootDir, DIR_SLAVES);
    repository.saveRepositoryDirectory(new RepositoryDirectory(rootDir, DIR_CLUSTERS));
    repository.createRepositoryDirectory(rootDir, DIR_TRANSFORMATIONS);
    repository.createRepositoryDirectory(rootDir, DIR_JOBS);
    rootDir = repository.loadRepositoryDirectoryTree();
    assertNotNull(rootDir.findDirectory(DIR_CONNECTIONS));
    assertNotNull(rootDir.findDirectory(DIR_SCHEMAS));
    assertNotNull(rootDir.findDirectory(DIR_SLAVES));
    assertNotNull(rootDir.findDirectory(DIR_CLUSTERS));
    assertNotNull(rootDir.findDirectory(DIR_TRANSFORMATIONS));
    assertNotNull(rootDir.findDirectory(DIR_JOBS));

    RepositoryDirectory tmpDir = repository.createRepositoryDirectory(rootDir, DIR_TMP);
    repository.deleteRepositoryDirectory(tmpDir);
    rootDir = repository.loadRepositoryDirectoryTree();
    assertNull(rootDir.findDirectory(DIR_TMP));

    String[] dirs = repository.getDirectoryNames(rootDir.getObjectId());
    assertEquals(6, dirs.length);
    boolean foundDir = false;
    for (String dir : dirs) {
      if (dir.equals(DIR_CONNECTIONS)) {
        foundDir = true;
        break;
      }
    }
    assertTrue(foundDir);
  }

  /**
   * save(job)
   * loadJob()
   * exists()
   * deleteJob()
   * getJobNames()
   * getJobObjects()
   * getJobId()
   */
  @Test
  public void testJobs() throws Exception {
    RepositoryDirectory rootDir = initRepo();
    JobMeta jobMeta = createJobMeta();
    RepositoryDirectory jobsDir = rootDir.findDirectory(DIR_JOBS);
    repository.save(jobMeta, VERSION_COMMENT_V1, null);
    assertNotNull(jobMeta.getObjectId());
    ObjectRevision version = jobMeta.getObjectRevision();
    assertNotNull(version);
    assertEquals(VERSION_COMMENT_V1, version.getComment());
    assertEquals(VERSION_LABEL_V1, version.getName());
    assertTrue(repository.exists(EXP_JOB_NAME, jobsDir, RepositoryObjectType.JOB));

    JobMeta fetchedJob = repository.loadJob(EXP_JOB_NAME, jobsDir, null, null);
    assertEquals(EXP_JOB_NAME, fetchedJob.getName());
    assertEquals(EXP_JOB_DESC, fetchedJob.getDescription());
    assertEquals(EXP_JOB_EXTENDED_DESC, fetchedJob.getExtendedDescription());
    assertEquals(jobsDir.getPath(), fetchedJob.getRepositoryDirectory().getPath());
    assertEquals(EXP_JOB_VERSION, fetchedJob.getJobversion());
    assertEquals(EXP_JOB_STATUS, fetchedJob.getJobstatus());
    assertEquals(EXP_JOB_CREATED_USER, fetchedJob.getCreatedUser());
    assertEquals(EXP_JOB_CREATED_DATE, fetchedJob.getCreatedDate());
    assertEquals(EXP_JOB_MOD_USER, fetchedJob.getModifiedUser());
    assertEquals(EXP_JOB_MOD_DATE, fetchedJob.getModifiedDate());
    assertEquals(1, fetchedJob.listParameters().length);
    assertEquals(EXP_JOB_PARAM_1_DEF, fetchedJob.getParameterDefault(EXP_JOB_PARAM_1_NAME));
    assertEquals(EXP_JOB_PARAM_1_DESC, fetchedJob.getParameterDescription(EXP_JOB_PARAM_1_NAME));
    JobLogTable jobLogTable = fetchedJob.getJobLogTable();
    // TODO mlowery why doesn't this work?
    //    assertEquals(EXP_LOG_TABLE_CONN_NAME, jobLogTable.getConnectionName());
    //    assertEquals(EXP_JOB_LOG_TABLE_INTERVAL, jobLogTable.getLogInterval());
    //    assertEquals(EXP_LOG_TABLE_SCHEMA_NAME, jobLogTable.getSchemaName());
    //    assertEquals(EXP_LOG_TABLE_SIZE_LIMIT, jobLogTable.getLogSizeLimit());
    //    assertEquals(EXP_LOG_TABLE_TABLE_NAME, jobLogTable.getTableName());
    //    assertEquals(EXP_LOG_TABLE_TIMEOUT_IN_DAYS, jobLogTable.getTimeoutInDays());
    JobEntryLogTable jobEntryLogTable = fetchedJob.getJobEntryLogTable();
    // TODO mlowery why doesn't this work?
    //    assertEquals(EXP_LOG_TABLE_CONN_NAME, jobEntryLogTable.getConnectionName());
    //    assertEquals(EXP_LOG_TABLE_SCHEMA_NAME, jobEntryLogTable.getSchemaName());
    //    assertEquals(EXP_LOG_TABLE_TABLE_NAME, jobEntryLogTable.getTableName());
    //    assertEquals(EXP_LOG_TABLE_TIMEOUT_IN_DAYS, jobEntryLogTable.getTimeoutInDays());
    ChannelLogTable channelLogTable = fetchedJob.getChannelLogTable();
    // TODO mlowery why doesn't this work?
    //    assertEquals(EXP_LOG_TABLE_CONN_NAME, channelLogTable.getConnectionName());
    //    assertEquals(EXP_LOG_TABLE_SCHEMA_NAME, channelLogTable.getSchemaName());
    //    assertEquals(EXP_LOG_TABLE_TABLE_NAME, channelLogTable.getTableName());
    //    assertEquals(EXP_LOG_TABLE_TIMEOUT_IN_DAYS, channelLogTable.getTimeoutInDays());
    assertEquals(EXP_JOB_BATCH_ID_PASSED, fetchedJob.isBatchIdPassed());
    assertEquals(EXP_JOB_SHARED_OBJECTS_FILE, fetchedJob.getSharedObjectsFile());
    assertEquals(2, fetchedJob.getJobCopies().size());
    assertEquals("JobEntryAttributeTester", fetchedJob.getJobEntry(0).getEntry().getTypeId());
    assertEquals(EXP_JOB_ENTRY_1_COPY_X_LOC, fetchedJob.getJobEntry(0).getLocation().x);
    assertEquals(EXP_JOB_ENTRY_1_COPY_Y_LOC, fetchedJob.getJobEntry(0).getLocation().y);
    assertEquals("JobEntryAttributeTester", fetchedJob.getJobEntry(1).getEntry().getTypeId());
    assertEquals(EXP_JOB_ENTRY_2_COPY_X_LOC, fetchedJob.getJobEntry(1).getLocation().x);
    assertEquals(EXP_JOB_ENTRY_2_COPY_Y_LOC, fetchedJob.getJobEntry(1).getLocation().y);
    assertEquals(1, fetchedJob.getJobhops().size());
    assertEquals(EXP_JOB_ENTRY_1_NAME, fetchedJob.getJobHop(0).getFromEntry().getEntry().getName());
    assertEquals("JobEntryAttributeTester", fetchedJob.getJobHop(0).getFromEntry().getEntry().getTypeId());
    assertEquals(EXP_JOB_ENTRY_2_NAME, fetchedJob.getJobHop(0).getToEntry().getEntry().getName());
    assertEquals("JobEntryAttributeTester", fetchedJob.getJobHop(0).getToEntry().getEntry().getTypeId());
    assertEquals(1, fetchedJob.getNotes().size());
    assertEquals(EXP_NOTEPAD_NOTE, fetchedJob.getNote(0).getNote());
    assertEquals(EXP_NOTEPAD_X, fetchedJob.getNote(0).getLocation().x);
    assertEquals(EXP_NOTEPAD_Y, fetchedJob.getNote(0).getLocation().y);
    assertEquals(EXP_NOTEPAD_WIDTH, fetchedJob.getNote(0).getWidth());
    assertEquals(EXP_NOTEPAD_HEIGHT, fetchedJob.getNote(0).getHeight());

    jobMeta.setDescription(EXP_JOB_DESC_V2);
    repository.save(jobMeta, VERSION_COMMENT_V2, null);
    assertEquals(VERSION_COMMENT_V2, jobMeta.getObjectRevision().getComment());
    fetchedJob = repository.loadJob(EXP_JOB_NAME, jobsDir, null, null);
    assertEquals(EXP_JOB_DESC_V2, fetchedJob.getDescription());
    fetchedJob = repository.loadJob(EXP_JOB_NAME, jobsDir, null, VERSION_LABEL_V1);
    assertEquals(EXP_JOB_DESC, fetchedJob.getDescription());

    assertEquals(jobMeta.getObjectId(), repository.getJobId(EXP_JOB_NAME, jobsDir));

    assertEquals(1, repository.getJobObjects(jobsDir.getObjectId(), false).size());
    assertEquals(1, repository.getJobObjects(jobsDir.getObjectId(), true).size());
    assertEquals(jobMeta.getName(), repository.getJobObjects(jobsDir.getObjectId(), false).get(0).getName());

    assertEquals(1, repository.getJobNames(jobsDir.getObjectId(), false).length);
    assertEquals(1, repository.getJobNames(jobsDir.getObjectId(), true).length);
    assertEquals(jobMeta.getName(), repository.getJobNames(jobsDir.getObjectId(), false)[0]);

    repository.deleteJob(jobMeta.getObjectId());
    assertFalse(repository.exists(EXP_JOB_NAME, jobsDir, RepositoryObjectType.JOB));

    assertEquals(jobMeta.getObjectId(), repository.getJobId(EXP_JOB_NAME, jobsDir));

    assertEquals(0, repository.getJobObjects(jobsDir.getObjectId(), false).size());
    assertEquals(1, repository.getJobObjects(jobsDir.getObjectId(), true).size());
    assertEquals(jobMeta.getName(), repository.getJobObjects(jobsDir.getObjectId(), true).get(0).getName());

    assertEquals(0, repository.getJobNames(jobsDir.getObjectId(), false).length);
    assertEquals(1, repository.getJobNames(jobsDir.getObjectId(), true).length);
    assertEquals(jobMeta.getName(), repository.getJobNames(jobsDir.getObjectId(), true)[0]);
  }

  private JobMeta createJobMeta() throws Exception {
    RepositoryDirectory rootDir = initRepo();
    JobMeta jobMeta = new JobMeta();
    jobMeta.setName(EXP_JOB_NAME);
    jobMeta.setDescription(EXP_JOB_DESC);
    jobMeta.setExtendedDescription(EXP_JOB_EXTENDED_DESC);
    jobMeta.setRepositoryDirectory(rootDir.findDirectory(DIR_JOBS));
    jobMeta.setJobversion(EXP_JOB_VERSION);
    jobMeta.setJobstatus(EXP_JOB_STATUS);
    jobMeta.setCreatedUser(EXP_JOB_CREATED_USER);
    jobMeta.setCreatedDate(EXP_JOB_CREATED_DATE);
    jobMeta.setModifiedUser(EXP_JOB_MOD_USER);
    jobMeta.setModifiedDate(EXP_JOB_MOD_DATE);
    jobMeta.addParameterDefinition(EXP_JOB_PARAM_1_NAME, EXP_JOB_PARAM_1_DEF, EXP_JOB_PARAM_1_DESC);
    // TODO mlowery other jobLogTable fields could be set for testing here    
    JobLogTable jobLogTable = JobLogTable.getDefault(jobMeta, jobMeta);
    jobLogTable.setConnectionName(EXP_JOB_LOG_TABLE_CONN_NAME);
    jobLogTable.setLogInterval(EXP_JOB_LOG_TABLE_INTERVAL);
    jobLogTable.setSchemaName(EXP_JOB_LOG_TABLE_SCHEMA_NAME);
    jobLogTable.setLogSizeLimit(EXP_JOB_LOG_TABLE_SIZE_LIMIT);
    jobLogTable.setTableName(EXP_JOB_LOG_TABLE_TABLE_NAME);
    jobLogTable.setTimeoutInDays(EXP_JOB_LOG_TABLE_TIMEOUT_IN_DAYS);
    jobMeta.setJobLogTable(jobLogTable);
    // TODO mlowery other jobEntryLogTable fields could be set for testing here    
    JobEntryLogTable jobEntryLogTable = JobEntryLogTable.getDefault(jobMeta, jobMeta);
    jobEntryLogTable.setConnectionName(EXP_JOB_LOG_TABLE_CONN_NAME);
    jobEntryLogTable.setSchemaName(EXP_JOB_LOG_TABLE_SCHEMA_NAME);
    jobEntryLogTable.setTableName(EXP_JOB_LOG_TABLE_TABLE_NAME);
    jobEntryLogTable.setTimeoutInDays(EXP_JOB_LOG_TABLE_TIMEOUT_IN_DAYS);
    jobMeta.setJobEntryLogTable(jobEntryLogTable);
    // TODO mlowery other channelLogTable fields could be set for testing here    
    ChannelLogTable channelLogTable = ChannelLogTable.getDefault(jobMeta, jobMeta);
    channelLogTable.setConnectionName(EXP_JOB_LOG_TABLE_CONN_NAME);
    channelLogTable.setSchemaName(EXP_JOB_LOG_TABLE_SCHEMA_NAME);
    channelLogTable.setTableName(EXP_JOB_LOG_TABLE_TABLE_NAME);
    channelLogTable.setTimeoutInDays(EXP_JOB_LOG_TABLE_TIMEOUT_IN_DAYS);
    jobMeta.setChannelLogTable(channelLogTable);
    jobMeta.setBatchIdPassed(EXP_JOB_BATCH_ID_PASSED);
    jobMeta.setSharedObjectsFile(EXP_JOB_SHARED_OBJECTS_FILE);
    JobEntryCopy jobEntryCopy1 = createJobEntry1Copy();
    jobMeta.addJobEntry(jobEntryCopy1);
    JobEntryCopy jobEntryCopy2 = createJobEntry2Copy();
    jobMeta.addJobEntry(jobEntryCopy2);
    jobMeta.addJobHop(createJobHopMeta(jobEntryCopy1, jobEntryCopy2));
    jobMeta.addNote(createNotePadMeta());
    return jobMeta;
  }

  /**
   * save(trans)
   * loadTransformation()
   * exists()

   */
  @Test
  public void testTransformations() throws Exception {
    RepositoryDirectory rootDir = initRepo();
    TransMeta transMeta = createTransMeta();
    RepositoryDirectory transDir = rootDir.findDirectory(DIR_TRANSFORMATIONS);
    repository.save(transMeta, VERSION_COMMENT_V1, null);
    assertNotNull(transMeta.getObjectId());
    ObjectRevision version = transMeta.getObjectRevision();
    assertNotNull(version);
    assertEquals(VERSION_COMMENT_V1, version.getComment());
    assertEquals(VERSION_LABEL_V1, version.getName());
    assertTrue(repository.exists(EXP_TRANS_NAME, transDir, RepositoryObjectType.TRANSFORMATION));

    TransMeta fetchedTrans = repository.loadTransformation(EXP_TRANS_NAME, transDir, null, false, null);
    assertEquals(EXP_TRANS_NAME, fetchedTrans.getName());
    assertEquals(EXP_TRANS_DESC, fetchedTrans.getDescription());
    assertEquals(EXP_TRANS_EXTENDED_DESC, fetchedTrans.getExtendedDescription());
    assertEquals(transDir.getPath(), fetchedTrans.getRepositoryDirectory().getPath());
    assertEquals(EXP_TRANS_VERSION, fetchedTrans.getTransversion());
    assertEquals(EXP_TRANS_STATUS, fetchedTrans.getTransstatus());
    assertEquals(EXP_TRANS_CREATED_USER, fetchedTrans.getCreatedUser());
    assertEquals(EXP_TRANS_CREATED_DATE, fetchedTrans.getCreatedDate());
    assertEquals(EXP_TRANS_MOD_USER, fetchedTrans.getModifiedUser());
    assertEquals(EXP_TRANS_MOD_DATE, fetchedTrans.getModifiedDate());
    assertEquals(1, fetchedTrans.listParameters().length);
    assertEquals(EXP_TRANS_PARAM_1_DEF, fetchedTrans.getParameterDefault(EXP_TRANS_PARAM_1_NAME));
    assertEquals(EXP_TRANS_PARAM_1_DESC, fetchedTrans.getParameterDescription(EXP_TRANS_PARAM_1_NAME));
    TransLogTable transLogTable = fetchedTrans.getTransLogTable();
    // TODO mlowery why doesn't this work?
    //    assertEquals(EXP_TRANS_LOG_TABLE_CONN_NAME, transLogTable.getConnectionName());
    //    assertEquals(EXP_TRANS_LOG_TABLE_INTERVAL, transLogTable.getLogInterval());
    //    assertEquals(EXP_TRANS_LOG_TABLE_SCHEMA_NAME, transLogTable.getSchemaName());
    //    assertEquals(EXP_TRANS_LOG_TABLE_SIZE_LIMIT, transLogTable.getLogSizeLimit());
    //    assertEquals(EXP_TRANS_LOG_TABLE_TABLE_NAME, transLogTable.getTableName());
    //    assertEquals(EXP_TRANS_LOG_TABLE_TIMEOUT_IN_DAYS, transLogTable.getTimeoutInDays());
    PerformanceLogTable perfLogTable = fetchedTrans.getPerformanceLogTable();
    // TODO mlowery why doesn't this work?
    //    assertEquals(EXP_TRANS_LOG_TABLE_CONN_NAME, perfLogTable.getConnectionName());
    //    assertEquals(EXP_TRANS_LOG_TABLE_INTERVAL, perfLogTable.getLogInterval());
    //    assertEquals(EXP_TRANS_LOG_TABLE_SCHEMA_NAME, perfLogTable.getSchemaName());
    //    assertEquals(EXP_TRANS_LOG_TABLE_TABLE_NAME, perfLogTable.getTableName());
    //    assertEquals(EXP_TRANS_LOG_TABLE_TIMEOUT_IN_DAYS, perfLogTable.getTimeoutInDays());
    ChannelLogTable channelLogTable = fetchedTrans.getChannelLogTable();
    // TODO mlowery why doesn't this work?
    //    assertEquals(EXP_TRANS_LOG_TABLE_CONN_NAME, channelLogTable.getConnectionName());
    //    assertEquals(EXP_TRANS_LOG_TABLE_SCHEMA_NAME, channelLogTable.getSchemaName());
    //    assertEquals(EXP_TRANS_LOG_TABLE_TABLE_NAME, channelLogTable.getTableName());
    //    assertEquals(EXP_TRANS_LOG_TABLE_TIMEOUT_IN_DAYS, channelLogTable.getTimeoutInDays());
    StepLogTable stepLogTable = fetchedTrans.getStepLogTable();
    // TODO mlowery why doesn't this work?
    //    assertEquals(EXP_TRANS_LOG_TABLE_CONN_NAME, stepLogTable.getConnectionName());
    //    assertEquals(EXP_TRANS_LOG_TABLE_SCHEMA_NAME, stepLogTable.getSchemaName());
    //    assertEquals(EXP_TRANS_LOG_TABLE_TABLE_NAME, stepLogTable.getTableName());
    //    assertEquals(EXP_TRANS_LOG_TABLE_TIMEOUT_IN_DAYS, stepLogTable.getTimeoutInDays());
    assertEquals(EXP_DBMETA_NAME, fetchedTrans.getMaxDateConnection().getName());
    assertEquals(EXP_TRANS_MAX_DATE_TABLE, fetchedTrans.getMaxDateTable());
    assertEquals(EXP_TRANS_MAX_DATE_FIELD, fetchedTrans.getMaxDateField());
    assertEquals(EXP_TRANS_MAX_DATE_OFFSET, fetchedTrans.getMaxDateOffset(), 0);
    assertEquals(EXP_TRANS_MAX_DATE_DIFF, fetchedTrans.getMaxDateDifference(), 0);

    assertEquals(EXP_TRANS_SIZE_ROWSET, fetchedTrans.getSizeRowset());
    // TODO mlowery why don't next two sleep fields work?
    //    assertEquals(EXP_TRANS_SLEEP_TIME_EMPTY, fetchedTrans.getSleepTimeEmpty());
    //    assertEquals(EXP_TRANS_SLEEP_TIME_FULL, fetchedTrans.getSleepTimeFull());
    assertEquals(EXP_TRANS_USING_UNIQUE_CONN, fetchedTrans.isUsingUniqueConnections());
    assertEquals(EXP_TRANS_FEEDBACK_SHOWN, fetchedTrans.isFeedbackShown());
    assertEquals(EXP_TRANS_FEEDBACK_SIZE, fetchedTrans.getFeedbackSize());
    assertEquals(EXP_TRANS_USING_THREAD_PRIORITY_MGMT, fetchedTrans.isUsingThreadPriorityManagment());
    assertEquals(EXP_TRANS_SHARED_OBJECTS_FILE, fetchedTrans.getSharedObjectsFile());
    assertEquals(EXP_TRANS_CAPTURE_STEP_PERF_SNAPSHOTS, fetchedTrans.isCapturingStepPerformanceSnapShots());
    assertEquals(EXP_TRANS_STEP_PERF_CAP_DELAY, fetchedTrans.getStepPerformanceCapturingDelay());
    // TODO mlowery why doesn't this work?
    //    assertEquals(1, fetchedTrans.getDependencies().size());
    //    assertEquals(EXP_DBMETA_NAME, fetchedTrans.getDependency(0).getDatabase().getName());
    //    assertEquals(EXP_TRANS_DEP_TABLE_NAME, fetchedTrans.getDependency(0).getTablename());
    //    assertEquals(EXP_TRANS_DEP_FIELD_NAME, fetchedTrans.getDependency(0).getFieldname());
    
    assertEquals(2, fetchedTrans.getSteps().size());
    assertEquals(EXP_TRANS_STEP_1_NAME, fetchedTrans.getStep(0).getName());
    assertEquals(EXP_TRANS_STEP_2_NAME, fetchedTrans.getStep(1).getName());
    
    assertEquals(EXP_TRANS_STEP_1_NAME, fetchedTrans.getTransHop(0).getFromStep().getName());
    assertEquals(EXP_TRANS_STEP_2_NAME, fetchedTrans.getTransHop(0).getToStep().getName());
  }

  private TransMeta createTransMeta() throws Exception {
    RepositoryDirectory rootDir = initRepo();
    TransMeta transMeta = new TransMeta();
    transMeta.setName(EXP_TRANS_NAME);
    transMeta.setDescription(EXP_TRANS_DESC);
    transMeta.setExtendedDescription(EXP_TRANS_EXTENDED_DESC);
    transMeta.setRepositoryDirectory(rootDir.findDirectory(DIR_TRANSFORMATIONS));
    transMeta.setTransversion(EXP_TRANS_VERSION);
    transMeta.setTransstatus(EXP_TRANS_STATUS);
    transMeta.setCreatedUser(EXP_TRANS_CREATED_USER);
    transMeta.setCreatedDate(EXP_TRANS_CREATED_DATE);
    transMeta.setModifiedUser(EXP_TRANS_MOD_USER);
    transMeta.setModifiedDate(EXP_TRANS_MOD_DATE);
    transMeta.addParameterDefinition(EXP_TRANS_PARAM_1_NAME, EXP_TRANS_PARAM_1_DEF, EXP_TRANS_PARAM_1_DESC);

    // TODO mlowery other transLogTable fields could be set for testing here  
    TransLogTable transLogTable = TransLogTable.getDefault(transMeta, transMeta);
    transLogTable.setConnectionName(EXP_TRANS_LOG_TABLE_CONN_NAME);
    transLogTable.setLogInterval(EXP_TRANS_LOG_TABLE_INTERVAL);
    transLogTable.setSchemaName(EXP_TRANS_LOG_TABLE_SCHEMA_NAME);
    transLogTable.setLogSizeLimit(EXP_TRANS_LOG_TABLE_SIZE_LIMIT);
    transLogTable.setTableName(EXP_TRANS_LOG_TABLE_TABLE_NAME);
    transLogTable.setTimeoutInDays(EXP_TRANS_LOG_TABLE_TIMEOUT_IN_DAYS);
    transMeta.setTransLogTable(transLogTable);
    // TODO mlowery other perfLogTable fields could be set for testing here  
    PerformanceLogTable perfLogTable = PerformanceLogTable.getDefault(transMeta, transMeta);
    perfLogTable.setConnectionName(EXP_TRANS_LOG_TABLE_CONN_NAME);
    perfLogTable.setLogInterval(EXP_TRANS_LOG_TABLE_INTERVAL);
    perfLogTable.setSchemaName(EXP_TRANS_LOG_TABLE_SCHEMA_NAME);
    perfLogTable.setTableName(EXP_TRANS_LOG_TABLE_TABLE_NAME);
    perfLogTable.setTimeoutInDays(EXP_TRANS_LOG_TABLE_TIMEOUT_IN_DAYS);
    transMeta.setPerformanceLogTable(perfLogTable);
    // TODO mlowery other channelLogTable fields could be set for testing here    
    ChannelLogTable channelLogTable = ChannelLogTable.getDefault(transMeta, transMeta);
    channelLogTable.setConnectionName(EXP_TRANS_LOG_TABLE_CONN_NAME);
    channelLogTable.setSchemaName(EXP_TRANS_LOG_TABLE_SCHEMA_NAME);
    channelLogTable.setTableName(EXP_TRANS_LOG_TABLE_TABLE_NAME);
    channelLogTable.setTimeoutInDays(EXP_TRANS_LOG_TABLE_TIMEOUT_IN_DAYS);
    transMeta.setChannelLogTable(channelLogTable);
    // TODO mlowery other stepLogTable fields could be set for testing here
    StepLogTable stepLogTable = StepLogTable.getDefault(transMeta, transMeta);
    stepLogTable.setConnectionName(EXP_TRANS_LOG_TABLE_CONN_NAME);
    stepLogTable.setSchemaName(EXP_TRANS_LOG_TABLE_SCHEMA_NAME);
    stepLogTable.setTableName(EXP_TRANS_LOG_TABLE_TABLE_NAME);
    stepLogTable.setTimeoutInDays(EXP_TRANS_LOG_TABLE_TIMEOUT_IN_DAYS);
    transMeta.setStepLogTable(stepLogTable);
    DatabaseMeta dbMeta = createDatabaseMeta();
    // dbMeta must be saved so that it gets an ID
    repository.save(dbMeta, VERSION_COMMENT_V1, null);
    transMeta.setMaxDateConnection(dbMeta);
    transMeta.setMaxDateTable(EXP_TRANS_MAX_DATE_TABLE);
    transMeta.setMaxDateField(EXP_TRANS_MAX_DATE_FIELD);
    transMeta.setMaxDateOffset(EXP_TRANS_MAX_DATE_OFFSET);
    transMeta.setMaxDateDifference(EXP_TRANS_MAX_DATE_DIFF);
    transMeta.setSizeRowset(EXP_TRANS_SIZE_ROWSET);
    transMeta.setSleepTimeEmpty(EXP_TRANS_SLEEP_TIME_EMPTY);
    transMeta.setSleepTimeFull(EXP_TRANS_SLEEP_TIME_FULL);
    transMeta.setUsingUniqueConnections(EXP_TRANS_USING_UNIQUE_CONN);
    transMeta.setFeedbackShown(EXP_TRANS_FEEDBACK_SHOWN);
    transMeta.setFeedbackSize(EXP_TRANS_FEEDBACK_SIZE);
    transMeta.setUsingThreadPriorityManagment(EXP_TRANS_USING_THREAD_PRIORITY_MGMT);
    transMeta.setSharedObjectsFile(EXP_TRANS_SHARED_OBJECTS_FILE);
    transMeta.setCapturingStepPerformanceSnapShots(EXP_TRANS_CAPTURE_STEP_PERF_SNAPSHOTS);
    transMeta.setStepPerformanceCapturingDelay(EXP_TRANS_STEP_PERF_CAP_DELAY);
    transMeta.addDependency(new TransDependency(dbMeta, EXP_TRANS_DEP_TABLE_NAME, EXP_TRANS_DEP_FIELD_NAME));

    StepMeta step1 = createStepMeta1();
    transMeta.addStep(step1);
    StepMeta step2 = createStepMeta2();
    transMeta.addStep(step2);
    transMeta.addTransHop(createTransHopMeta(step1, step2));
    return transMeta;
  }

  private PartitionSchema createPartitionSchema() throws Exception {
    PartitionSchema partSchema = new PartitionSchema();
    partSchema.setName(EXP_PART_SCHEMA_NAME);
    partSchema.setDescription(EXP_PART_SCHEMA_DESC);
    partSchema.setPartitionIDs(Arrays.asList(new String[] { EXP_PART_SCHEMA_PARTID_1, EXP_PART_SCHEMA_PARTID_2 }));
    partSchema.setDynamicallyDefined(EXP_PART_SCHEMA_DYN_DEF);
    partSchema.setNumberOfPartitionsPerSlave(EXP_PART_SCHEMA_PART_PER_SLAVE_COUNT);
    return partSchema;
  }

  /**
   * save(partitionSchema)
   * exists()
   * loadPartitionSchema()
   * getPartitionSchemaID()
   * getPartitionSchemaIDs()
   * getPartitionSchemaNames()
   */
  @Test
  public void testPartitionSchemas() throws Exception {
    RepositoryDirectory rootDir = initRepo();
    PartitionSchema partSchema = createPartitionSchema();
    repository.save(partSchema, VERSION_COMMENT_V1, null);
    assertNotNull(partSchema.getObjectId());
    ObjectRevision version = partSchema.getObjectRevision();
    assertNotNull(version);
    assertEquals(VERSION_COMMENT_V1, version.getComment());
    assertEquals(VERSION_LABEL_V1, version.getName());
    assertTrue(repository.exists(EXP_PART_SCHEMA_NAME, null, RepositoryObjectType.PARTITION_SCHEMA));

    PartitionSchema fetchedPartSchema = repository.loadPartitionSchema(partSchema.getObjectId(), null);
    assertEquals(EXP_PART_SCHEMA_NAME, fetchedPartSchema.getName());
    // TODO mlowery partitionSchema.getXML doesn't output desc either; should it?
    //    assertEquals(EXP_PART_SCHEMA_DESC, fetchedPartSchema.getDescription());

    assertEquals(Arrays.asList(new String[] { EXP_PART_SCHEMA_PARTID_1, EXP_PART_SCHEMA_PARTID_2 }), fetchedPartSchema
        .getPartitionIDs());
    assertEquals(EXP_PART_SCHEMA_DYN_DEF, fetchedPartSchema.isDynamicallyDefined());
    assertEquals(EXP_PART_SCHEMA_PART_PER_SLAVE_COUNT, fetchedPartSchema.getNumberOfPartitionsPerSlave());

    partSchema.setNumberOfPartitionsPerSlave(EXP_PART_SCHEMA_PART_PER_SLAVE_COUNT_V2);
    repository.save(partSchema, VERSION_COMMENT_V2, null);
    assertEquals(VERSION_COMMENT_V2, partSchema.getObjectRevision().getComment());
    fetchedPartSchema = repository.loadPartitionSchema(partSchema.getObjectId(), null);
    assertEquals(EXP_PART_SCHEMA_PART_PER_SLAVE_COUNT_V2, fetchedPartSchema.getNumberOfPartitionsPerSlave());
    fetchedPartSchema = repository.loadPartitionSchema(partSchema.getObjectId(), VERSION_LABEL_V1);
    assertEquals(EXP_PART_SCHEMA_PART_PER_SLAVE_COUNT, fetchedPartSchema.getNumberOfPartitionsPerSlave());

    assertEquals(partSchema.getObjectId(), repository.getPartitionSchemaID(EXP_PART_SCHEMA_NAME));

    assertEquals(1, repository.getPartitionSchemaIDs(false).length);
    assertEquals(1, repository.getPartitionSchemaIDs(true).length);
    assertEquals(partSchema.getObjectId(), repository.getPartitionSchemaIDs(false)[0]);

    assertEquals(1, repository.getPartitionSchemaNames(false).length);
    assertEquals(1, repository.getPartitionSchemaNames(true).length);
    assertEquals(EXP_PART_SCHEMA_NAME, repository.getPartitionSchemaNames(false)[0]);

    repository.deletePartitionSchema(partSchema.getObjectId());
    assertFalse(repository.exists(EXP_PART_SCHEMA_NAME, null, RepositoryObjectType.PARTITION_SCHEMA));

    assertEquals(partSchema.getObjectId(), repository.getPartitionSchemaID(EXP_PART_SCHEMA_NAME));

    assertEquals(0, repository.getPartitionSchemaIDs(false).length);
    assertEquals(1, repository.getPartitionSchemaIDs(true).length);
    assertEquals(partSchema.getObjectId(), repository.getPartitionSchemaIDs(true)[0]);

    assertEquals(0, repository.getPartitionSchemaNames(false).length);
    assertEquals(1, repository.getPartitionSchemaNames(true).length);
    assertEquals(EXP_PART_SCHEMA_NAME, repository.getPartitionSchemaNames(true)[0]);
  }

  /**
   * save(clusterSchema)
   * exists()
   * loadClusterSchema()
   * getClusterID()
   * getClusterIDs()
   * getClusterNames()
   */
  @Test
  public void testClusterSchemas() throws Exception {
    RepositoryDirectory rootDir = initRepo();
    ClusterSchema clusterSchema = createClusterSchema();
    repository.save(clusterSchema, VERSION_COMMENT_V1, null);
    assertNotNull(clusterSchema.getObjectId());
    ObjectRevision version = clusterSchema.getObjectRevision();
    assertNotNull(version);
    assertEquals(VERSION_COMMENT_V1, version.getComment());
    assertEquals(VERSION_LABEL_V1, version.getName());
    assertTrue(repository.exists(EXP_CLUSTER_SCHEMA_NAME, null, RepositoryObjectType.CLUSTER_SCHEMA));

    ClusterSchema fetchedClusterSchema = repository.loadClusterSchema(clusterSchema.getObjectId(), repository
        .getSlaveServers(), null);
    assertEquals(EXP_CLUSTER_SCHEMA_NAME, fetchedClusterSchema.getName());
    // TODO mlowery clusterSchema.getXML doesn't output desc either; should it?
    //    assertEquals(EXP_CLUSTER_SCHEMA_DESC, fetchedClusterSchema.getDescription());

    assertEquals(EXP_CLUSTER_SCHEMA_BASE_PORT, fetchedClusterSchema.getBasePort());
    assertEquals(EXP_CLUSTER_SCHEMA_SOCKETS_BUFFER_SIZE, fetchedClusterSchema.getSocketsBufferSize());
    assertEquals(EXP_CLUSTER_SCHEMA_SOCKETS_FLUSH_INTERVAL, fetchedClusterSchema.getSocketsFlushInterval());
    assertEquals(EXP_CLUSTER_SCHEMA_SOCKETS_COMPRESSED, fetchedClusterSchema.isSocketsCompressed());
    assertEquals(EXP_CLUSTER_SCHEMA_DYN, fetchedClusterSchema.isDynamic());
    assertEquals(1, fetchedClusterSchema.getSlaveServers().size());
    assertEquals(EXP_SLAVE_NAME, fetchedClusterSchema.getSlaveServers().get(0).getName());

    // versioning test
    clusterSchema.setBasePort(EXP_CLUSTER_SCHEMA_BASE_PORT_V2);
    repository.save(clusterSchema, VERSION_COMMENT_V2, null);
    assertEquals(VERSION_COMMENT_V2, clusterSchema.getObjectRevision().getComment());
    fetchedClusterSchema = repository
        .loadClusterSchema(clusterSchema.getObjectId(), repository.getSlaveServers(), null);
    assertEquals(EXP_CLUSTER_SCHEMA_BASE_PORT_V2, fetchedClusterSchema.getBasePort());
    fetchedClusterSchema = repository.loadClusterSchema(clusterSchema.getObjectId(), repository.getSlaveServers(),
        VERSION_LABEL_V1);
    assertEquals(EXP_CLUSTER_SCHEMA_BASE_PORT, fetchedClusterSchema.getBasePort());

    assertEquals(clusterSchema.getObjectId(), repository.getClusterID(EXP_CLUSTER_SCHEMA_NAME));

    assertEquals(1, repository.getClusterIDs(false).length);
    assertEquals(1, repository.getClusterIDs(true).length);
    assertEquals(clusterSchema.getObjectId(), repository.getClusterIDs(false)[0]);

    assertEquals(1, repository.getClusterNames(false).length);
    assertEquals(1, repository.getClusterNames(true).length);
    assertEquals(EXP_CLUSTER_SCHEMA_NAME, repository.getClusterNames(false)[0]);

    repository.deleteClusterSchema(clusterSchema.getObjectId());
    assertFalse(repository.exists(EXP_CLUSTER_SCHEMA_NAME, null, RepositoryObjectType.CLUSTER_SCHEMA));

    assertEquals(clusterSchema.getObjectId(), repository.getClusterID(EXP_CLUSTER_SCHEMA_NAME));

    assertEquals(0, repository.getClusterIDs(false).length);
    assertEquals(1, repository.getClusterIDs(true).length);
    assertEquals(clusterSchema.getObjectId(), repository.getClusterIDs(true)[0]);

    assertEquals(0, repository.getClusterNames(false).length);
    assertEquals(1, repository.getClusterNames(true).length);
    assertEquals(EXP_CLUSTER_SCHEMA_NAME, repository.getClusterNames(true)[0]);
  }

  private ClusterSchema createClusterSchema() throws Exception {
    ClusterSchema clusterSchema = new ClusterSchema();
    clusterSchema.setName(EXP_CLUSTER_SCHEMA_NAME);
    clusterSchema.setBasePort(EXP_CLUSTER_SCHEMA_BASE_PORT);
    clusterSchema.setSocketsBufferSize(EXP_CLUSTER_SCHEMA_SOCKETS_BUFFER_SIZE);
    clusterSchema.setSocketsFlushInterval(EXP_CLUSTER_SCHEMA_SOCKETS_FLUSH_INTERVAL);
    clusterSchema.setSocketsCompressed(EXP_CLUSTER_SCHEMA_SOCKETS_COMPRESSED);
    clusterSchema.setDynamic(EXP_CLUSTER_SCHEMA_DYN);
    SlaveServer slaveServer = createSlaveServer();
    repository.save(slaveServer, VERSION_COMMENT_V1, null);
    clusterSchema.setSlaveServers(Collections.singletonList(slaveServer));
    return clusterSchema;
  }

  private JobEntryInterface createJobEntry1() throws Exception {
    return new JobEntryAttributeTesterJobEntry(EXP_JOB_ENTRY_1_NAME);
  }

  private JobEntryInterface createJobEntry2() throws Exception {
    return new JobEntryAttributeTesterJobEntry(EXP_JOB_ENTRY_2_NAME);
  }

  private JobEntryCopy createJobEntry1Copy() throws Exception {
    JobEntryCopy copy = new JobEntryCopy(createJobEntry1());
    copy.setLocation(EXP_JOB_ENTRY_1_COPY_X_LOC, EXP_JOB_ENTRY_1_COPY_Y_LOC);
    return copy;
  }

  private JobEntryCopy createJobEntry2Copy() throws Exception {
    JobEntryCopy copy = new JobEntryCopy(createJobEntry2());
    copy.setLocation(EXP_JOB_ENTRY_2_COPY_X_LOC, EXP_JOB_ENTRY_2_COPY_Y_LOC);
    return copy;
  }

  private StepMeta createStepMeta1() throws Exception {
    return new StepMeta(EXP_TRANS_STEP_1_NAME, new TransStepAttributeTesterTransStep());
  }

  private StepMeta createStepMeta2() throws Exception {
    return new StepMeta(EXP_TRANS_STEP_2_NAME, new TransStepAttributeTesterTransStep());
  }


  
    private TransHopMeta createTransHopMeta(final StepMeta stepMeta1, final StepMeta stepMeta2) throws Exception {
      return new TransHopMeta(stepMeta1, stepMeta2);
    }

  private NotePadMeta createNotePadMeta() throws Exception {
    return new NotePadMeta(EXP_NOTEPAD_NOTE, EXP_NOTEPAD_X, EXP_NOTEPAD_Y, EXP_NOTEPAD_WIDTH, EXP_NOTEPAD_HEIGHT);
  }

  private JobHopMeta createJobHopMeta(final JobEntryCopy from, final JobEntryCopy to) throws Exception {
    return new JobHopMeta(from, to);
  }

  private DatabaseMeta createDatabaseMeta() throws Exception {
    DatabaseMeta dbMeta = new DatabaseMeta();
    dbMeta.setName(EXP_DBMETA_NAME);
    dbMeta.setHostname(EXP_DBMETA_HOSTNAME);
    dbMeta.setDatabaseType(EXP_DBMETA_TYPE);
    dbMeta.setAccessType(EXP_DBMETA_ACCESS);
    dbMeta.setDBName(EXP_DBMETA_DBNAME);
    dbMeta.setDBPort(EXP_DBMETA_PORT);
    dbMeta.setUsername(EXP_DBMETA_USERNAME);
    dbMeta.setPassword(EXP_DBMETA_PASSWORD);
    dbMeta.setServername(EXP_DBMETA_SERVERNAME);
    dbMeta.setDataTablespace(EXP_DBMETA_DATA_TABLESPACE);
    dbMeta.setIndexTablespace(EXP_DBMETA_INDEX_TABLESPACE);
    // TODO mlowery more testing on DatabaseMeta attributes/options
    return dbMeta;
  }

  /**
   * save(databaseMeta)
   * loadDatabaseMeta()
   * exists()
   * deleteDatabaseMeta()
   * getDatabaseID()
   * getDatabaseIDs()
   * getDatabaseNames()
   */
  @Test
  public void testDatabases() throws Exception {
    DatabaseMeta dbMeta = createDatabaseMeta();
    repository.save(dbMeta, VERSION_COMMENT_V1, null);
    assertNotNull(dbMeta.getObjectId());
    ObjectRevision version = dbMeta.getObjectRevision();
    assertNotNull(version);
    assertEquals(VERSION_COMMENT_V1, version.getComment());
    assertEquals(VERSION_LABEL_V1, version.getName());
    // setting repository directory on dbMeta is not supported; use null parent directory
    assertTrue(repository.exists(EXP_DBMETA_NAME, null, RepositoryObjectType.DATABASE));

    DatabaseMeta fetchedDatabase = repository.loadDatabaseMeta(dbMeta.getObjectId(), null);
    assertEquals(EXP_DBMETA_NAME, fetchedDatabase.getName());
    assertEquals(EXP_DBMETA_HOSTNAME, fetchedDatabase.getHostname());
    assertEquals(EXP_DBMETA_TYPE, fetchedDatabase.getDatabaseTypeDesc());
    assertEquals(EXP_DBMETA_ACCESS, fetchedDatabase.getAccessType());
    assertEquals(EXP_DBMETA_DBNAME, fetchedDatabase.getDatabaseName());
    assertEquals(EXP_DBMETA_PORT, fetchedDatabase.getDatabasePortNumberString());
    assertEquals(EXP_DBMETA_USERNAME, fetchedDatabase.getUsername());
    assertEquals(EXP_DBMETA_PASSWORD, fetchedDatabase.getPassword());
    assertEquals(EXP_DBMETA_SERVERNAME, fetchedDatabase.getServername());
    assertEquals(EXP_DBMETA_DATA_TABLESPACE, fetchedDatabase.getDataTablespace());
    assertEquals(EXP_DBMETA_INDEX_TABLESPACE, fetchedDatabase.getIndexTablespace());

    dbMeta.setHostname(EXP_DBMETA_HOSTNAME_V2);
    repository.save(dbMeta, VERSION_COMMENT_V2, null);
    assertEquals(VERSION_COMMENT_V2, dbMeta.getObjectRevision().getComment());
    fetchedDatabase = repository.loadDatabaseMeta(dbMeta.getObjectId(), null);
    assertEquals(EXP_DBMETA_HOSTNAME_V2, fetchedDatabase.getHostname());
    fetchedDatabase = repository.loadDatabaseMeta(dbMeta.getObjectId(), VERSION_LABEL_V1);
    assertEquals(EXP_DBMETA_HOSTNAME, fetchedDatabase.getHostname());

    assertEquals(dbMeta.getObjectId(), repository.getDatabaseID(EXP_DBMETA_NAME));
    assertEquals(1, repository.getDatabaseIDs(false).length);
    assertEquals(1, repository.getDatabaseIDs(true).length);
    assertEquals(dbMeta.getObjectId(), repository.getDatabaseIDs(false)[0]);

    assertEquals(1, repository.getDatabaseNames(false).length);
    assertEquals(1, repository.getDatabaseNames(true).length);
    assertEquals(EXP_DBMETA_NAME, repository.getDatabaseNames(false)[0]);

    repository.deleteDatabaseMeta(EXP_DBMETA_NAME);
    assertFalse(repository.exists(EXP_DBMETA_NAME, null, RepositoryObjectType.DATABASE));

    assertEquals(dbMeta.getObjectId(), repository.getDatabaseID(EXP_DBMETA_NAME));

    assertEquals(0, repository.getDatabaseIDs(false).length);
    assertEquals(1, repository.getDatabaseIDs(true).length);
    assertEquals(dbMeta.getObjectId(), repository.getDatabaseIDs(true)[0]);

    assertEquals(0, repository.getDatabaseNames(false).length);
    assertEquals(1, repository.getDatabaseNames(true).length);
    assertEquals(EXP_DBMETA_NAME, repository.getDatabaseNames(true)[0]);
  }

  /**
   * save(slave)
   * loadSlaveServer()
   * exists()
   * deleteSlave()
   * getSlaveID()
   * getSlaveIDs()
   * getSlaveNames()
   * getSlaveServers()
   */
  @Test
  public void testSlaves() throws Exception {
    SlaveServer slave = createSlaveServer();
    repository.save(slave, VERSION_COMMENT_V1, null);
    assertNotNull(slave.getObjectId());
    ObjectRevision version = slave.getObjectRevision();
    assertNotNull(version);
    assertEquals(VERSION_COMMENT_V1, version.getComment());
    assertEquals(VERSION_LABEL_V1, version.getName());
    // setting repository directory on slave is not supported; use null parent directory
    assertTrue(repository.exists(EXP_SLAVE_NAME, null, RepositoryObjectType.SLAVE_SERVER));

    SlaveServer fetchedSlave = repository.loadSlaveServer(slave.getObjectId(), null);
    assertEquals(EXP_SLAVE_NAME, fetchedSlave.getName());
    assertEquals(EXP_SLAVE_HOSTNAME, fetchedSlave.getHostname());
    assertEquals(EXP_SLAVE_PORT, fetchedSlave.getPort());
    assertEquals(EXP_SLAVE_USERNAME, fetchedSlave.getUsername());
    assertEquals(EXP_SLAVE_PASSWORD, fetchedSlave.getPassword());
    assertEquals(EXP_SLAVE_PROXY_HOSTNAME, fetchedSlave.getProxyHostname());
    assertEquals(EXP_SLAVE_PROXY_PORT, fetchedSlave.getProxyPort());
    assertEquals(EXP_SLAVE_NON_PROXY_HOSTS, fetchedSlave.getNonProxyHosts());
    assertEquals(EXP_SLAVE_MASTER, fetchedSlave.isMaster());

    slave.setHostname(EXP_SLAVE_HOSTNAME_V2);
    repository.save(slave, VERSION_COMMENT_V2, null);
    assertEquals(VERSION_COMMENT_V2, slave.getObjectRevision().getComment());
    fetchedSlave = repository.loadSlaveServer(slave.getObjectId(), null);
    assertEquals(EXP_SLAVE_HOSTNAME_V2, fetchedSlave.getHostname());
    fetchedSlave = repository.loadSlaveServer(slave.getObjectId(), VERSION_LABEL_V1);
    assertEquals(EXP_SLAVE_HOSTNAME, fetchedSlave.getHostname());

    assertEquals(slave.getObjectId(), repository.getSlaveID(EXP_SLAVE_NAME));

    assertEquals(1, repository.getSlaveIDs(false).length);
    assertEquals(1, repository.getSlaveIDs(true).length);
    assertEquals(slave.getObjectId(), repository.getSlaveIDs(false)[0]);

    assertEquals(1, repository.getSlaveNames(false).length);
    assertEquals(1, repository.getSlaveNames(true).length);
    assertEquals(EXP_SLAVE_NAME, repository.getSlaveNames(false)[0]);

    assertEquals(1, repository.getSlaveServers().size());
    assertEquals(EXP_SLAVE_NAME, repository.getSlaveServers().get(0).getName());

    repository.deleteSlave(slave.getObjectId());
    assertFalse(repository.exists(EXP_SLAVE_NAME, null, RepositoryObjectType.SLAVE_SERVER));

    assertEquals(slave.getObjectId(), repository.getSlaveID(EXP_SLAVE_NAME));

    assertEquals(0, repository.getSlaveIDs(false).length);
    assertEquals(1, repository.getSlaveIDs(true).length);
    assertEquals(slave.getObjectId(), repository.getSlaveIDs(true)[0]);

    assertEquals(0, repository.getSlaveNames(false).length);
    assertEquals(1, repository.getSlaveNames(true).length);
    assertEquals(EXP_SLAVE_NAME, repository.getSlaveNames(true)[0]);

    assertEquals(0, repository.getSlaveServers().size());
  }

  private SlaveServer createSlaveServer() throws Exception {
    SlaveServer slaveServer = new SlaveServer();
    slaveServer.setName(EXP_SLAVE_NAME);
    slaveServer.setHostname(EXP_SLAVE_HOSTNAME);
    slaveServer.setPort(EXP_SLAVE_PORT);
    slaveServer.setUsername(EXP_SLAVE_USERNAME);
    slaveServer.setPassword(EXP_SLAVE_PASSWORD);
    slaveServer.setProxyHostname(EXP_SLAVE_PROXY_HOSTNAME);
    slaveServer.setProxyPort(EXP_SLAVE_PROXY_PORT);
    slaveServer.setNonProxyHosts(EXP_SLAVE_NON_PROXY_HOSTS);
    slaveServer.setMaster(EXP_SLAVE_MASTER);
    return slaveServer;
  }

  @Test
  @Ignore
  public void testCountNrJobEntryAttributes() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testCountNrStepAttributes() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testDeleteClusterSchema() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testDeletePartitionSchema() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testDeleteTransformation() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetJobLock() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetRevisions() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetSecurityProvider() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetStepAttributeBooleanObjectIdString() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetStepAttributeBooleanObjectIdIntString() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetStepAttributeBooleanObjectIdIntStringBoolean() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetStepAttributeIntegerObjectIdString() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetStepAttributeIntegerObjectIdIntString() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetStepAttributeStringObjectIdString() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetStepAttributeStringObjectIdIntString() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetTransformationID() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetTransformationLock() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetTransformationNames() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetTransformationObjects() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetVersionRegistry() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testInit() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testInsertJobEntryDatabase() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testInsertLogEntry() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testInsertStepDatabase() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testLoadConditionFromStepAttribute() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testLoadDatabaseMetaFromJobEntryAttribute() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testLoadDatabaseMetaFromStepAttribute() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testLockJob() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testLockTransformation() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testReadDatabases() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testReadJobMetaSharedObjects() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testReadTransSharedObjects() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testRenameDatabase() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testRenameJob() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testRenameRepositoryDirectory() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testRenameTransformation() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testSaveConditionStepAttribute() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testSaveDatabaseMetaJobEntryAttribute() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testSaveDatabaseMetaStepAttribute() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testSaveStepAttributeObjectIdObjectIdStringString() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testSaveStepAttributeObjectIdObjectIdStringBoolean() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testSaveStepAttributeObjectIdObjectIdStringLong() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testSaveStepAttributeObjectIdObjectIdStringDouble() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testSaveStepAttributeObjectIdObjectIdIntStringString() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testSaveStepAttributeObjectIdObjectIdIntStringBoolean() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testSaveStepAttributeObjectIdObjectIdIntStringLong() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testSaveStepAttributeObjectIdObjectIdIntStringDouble() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testUndeleteObject() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testUnlockJob() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testUnlockTransformation() throws Exception {
    fail("Not yet implemented");
  }

  public static interface EntryAndStepConstants {

    String ATTR_BOOL = "KDF";

    boolean VALUE_BOOL = true;

    String ATTR_BOOL_MULTI = "DFS";

    boolean VALUE_BOOL_MULTI_0 = true;

    boolean VALUE_BOOL_MULTI_1 = false;

    String ATTR_BOOL_NOEXIST = "IXS";

    boolean VALUE_BOOL_NOEXIST_DEF = true;

    String ATTR_INT = "TAS";

    int VALUE_INT = 4;

    String ATTR_INT_MULTI = "EZA";

    int VALUE_INT_MULTI_0 = 13;

    int VALUE_INT_MULTI_1 = 42;

    String ATTR_STRING = "YAZ";

    String VALUE_STRING = "sdfsdfsdfswe2222";

    String ATTR_STRING_MULTI = "LKS";

    String VALUE_STRING_MULTI_0 = "LKS";

    String VALUE_STRING_MULTI_1 = "LKS";
  }

  /**
   * Does assertions on all repository.getJobEntryAttribute* and repository.saveJobEntryAttribute* methods.
   */
  @Job(id = "JobEntryAttributeTester", image = "")
  public static class JobEntryAttributeTesterJobEntry extends JobEntryBase implements Cloneable, JobEntryInterface,
      EntryAndStepConstants {

    public JobEntryAttributeTesterJobEntry() {
      this("");
    }

    public JobEntryAttributeTesterJobEntry(final String name) {
      super(name, "");
    }

    @Override
    public void loadRep(final Repository rep, final ObjectId idJobentry, final List<DatabaseMeta> databases,
        final List<SlaveServer> slaveServers) throws KettleException {
      assertEquals(VALUE_BOOL, rep.getJobEntryAttributeBoolean(idJobentry, ATTR_BOOL));
      assertEquals(VALUE_BOOL_MULTI_0, rep.getJobEntryAttributeBoolean(idJobentry, 0, ATTR_BOOL_MULTI));
      assertEquals(VALUE_BOOL_MULTI_1, rep.getJobEntryAttributeBoolean(idJobentry, 1, ATTR_BOOL_MULTI));
      assertEquals(VALUE_BOOL_NOEXIST_DEF, rep.getJobEntryAttributeBoolean(idJobentry, ATTR_BOOL_NOEXIST,
          VALUE_BOOL_NOEXIST_DEF));
      assertEquals(VALUE_INT, rep.getJobEntryAttributeInteger(idJobentry, ATTR_INT));
      assertEquals(VALUE_INT_MULTI_0, rep.getJobEntryAttributeInteger(idJobentry, 0, ATTR_INT_MULTI));
      assertEquals(VALUE_INT_MULTI_1, rep.getJobEntryAttributeInteger(idJobentry, 1, ATTR_INT_MULTI));
      assertEquals(VALUE_STRING, rep.getJobEntryAttributeString(idJobentry, ATTR_STRING));
      assertEquals(VALUE_STRING_MULTI_0, rep.getJobEntryAttributeString(idJobentry, 0, ATTR_STRING_MULTI));
      assertEquals(VALUE_STRING_MULTI_1, rep.getJobEntryAttributeString(idJobentry, 1, ATTR_STRING_MULTI));
    }

    @Override
    public void saveRep(final Repository rep, final ObjectId idJob) throws KettleException {
      rep.saveJobEntryAttribute(idJob, getObjectId(), ATTR_BOOL, VALUE_BOOL);
      rep.saveJobEntryAttribute(idJob, getObjectId(), 0, ATTR_BOOL_MULTI, VALUE_BOOL_MULTI_0);
      rep.saveJobEntryAttribute(idJob, getObjectId(), 1, ATTR_BOOL_MULTI, VALUE_BOOL_MULTI_1);
      rep.saveJobEntryAttribute(idJob, getObjectId(), ATTR_INT, VALUE_INT);
      rep.saveJobEntryAttribute(idJob, getObjectId(), 0, ATTR_INT_MULTI, VALUE_INT_MULTI_0);
      rep.saveJobEntryAttribute(idJob, getObjectId(), 1, ATTR_INT_MULTI, VALUE_INT_MULTI_1);
      rep.saveJobEntryAttribute(idJob, getObjectId(), ATTR_STRING, VALUE_STRING);
      rep.saveJobEntryAttribute(idJob, getObjectId(), 0, ATTR_STRING_MULTI, VALUE_STRING_MULTI_0);
      rep.saveJobEntryAttribute(idJob, getObjectId(), 1, ATTR_STRING_MULTI, VALUE_STRING_MULTI_1);
    }

    public Result execute(final Result prevResult, final int nr) throws KettleException {
      throw new UnsupportedOperationException();
    }

    public void loadXML(final Node entrynode, final List<DatabaseMeta> databases, final List<SlaveServer> slaveServers,
        final Repository rep) throws KettleXMLException {
      throw new UnsupportedOperationException();
    }

  }

  @Step(name = "StepAttributeTester", image = "")
  public static class TransStepAttributeTesterTransStep extends BaseStepMeta implements StepMetaInterface,
      EntryAndStepConstants {

    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
        RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info) {
    }

    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
        TransMeta transMeta, Trans trans) {
      return null;

    }

    public StepDataInterface getStepData() {
      return null;

    }

    public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
        throws KettleXMLException {
    }

    public void readRep(Repository rep, ObjectId idStep, List<DatabaseMeta> databases, Map<String, Counter> counters)
        throws KettleException {
      assertEquals(VALUE_BOOL, rep.getStepAttributeBoolean(idStep, ATTR_BOOL));
      assertEquals(VALUE_BOOL_MULTI_0, rep.getStepAttributeBoolean(idStep, 0, ATTR_BOOL_MULTI));
      assertEquals(VALUE_BOOL_MULTI_1, rep.getStepAttributeBoolean(idStep, 1, ATTR_BOOL_MULTI));
      assertEquals(VALUE_BOOL_NOEXIST_DEF, rep.getStepAttributeBoolean(idStep, 0, ATTR_BOOL_NOEXIST,
          VALUE_BOOL_NOEXIST_DEF));
      assertEquals(VALUE_INT, rep.getStepAttributeInteger(idStep, ATTR_INT));
      assertEquals(VALUE_INT_MULTI_0, rep.getStepAttributeInteger(idStep, 0, ATTR_INT_MULTI));
      assertEquals(VALUE_INT_MULTI_1, rep.getStepAttributeInteger(idStep, 1, ATTR_INT_MULTI));
      assertEquals(VALUE_STRING, rep.getStepAttributeString(idStep, ATTR_STRING));
      assertEquals(VALUE_STRING_MULTI_0, rep.getStepAttributeString(idStep, 0, ATTR_STRING_MULTI));
      assertEquals(VALUE_STRING_MULTI_1, rep.getStepAttributeString(idStep, 1, ATTR_STRING_MULTI));
    }

    public void saveRep(Repository rep, ObjectId idTransformation, ObjectId idStep) throws KettleException {
      rep.saveStepAttribute(idTransformation, idStep, ATTR_BOOL, VALUE_BOOL);
      rep.saveStepAttribute(idTransformation, idStep, 0, ATTR_BOOL_MULTI, VALUE_BOOL_MULTI_0);
      rep.saveStepAttribute(idTransformation, idStep, 1, ATTR_BOOL_MULTI, VALUE_BOOL_MULTI_1);
      rep.saveStepAttribute(idTransformation, idStep, ATTR_INT, VALUE_INT);
      rep.saveStepAttribute(idTransformation, idStep, 0, ATTR_INT_MULTI, VALUE_INT_MULTI_0);
      rep.saveStepAttribute(idTransformation, idStep, 1, ATTR_INT_MULTI, VALUE_INT_MULTI_1);
      rep.saveStepAttribute(idTransformation, idStep, ATTR_STRING, VALUE_STRING);
      rep.saveStepAttribute(idTransformation, idStep, 0, ATTR_STRING_MULTI, VALUE_STRING_MULTI_0);
      rep.saveStepAttribute(idTransformation, idStep, 1, ATTR_STRING_MULTI, VALUE_STRING_MULTI_1);
    }

    public void setDefault() {
    }

  }
}
