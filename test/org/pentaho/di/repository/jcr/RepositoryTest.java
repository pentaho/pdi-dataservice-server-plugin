package org.pentaho.di.repository.jcr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jackrabbit.core.TransientRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.NotePadMeta;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.logging.ChannelLogTable;
import org.pentaho.di.core.logging.JobEntryLogTable;
import org.pentaho.di.core.logging.JobLogTable;
import org.pentaho.di.job.JobHopMeta;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.createfile.JobEntryCreateFile;
import org.pentaho.di.job.entries.deletefile.JobEntryDeleteFile;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectRevision;
import org.pentaho.di.repository.ProfileMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryCapabilities;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.di.repository.ProfileMeta.Permission;
import org.pentaho.di.trans.TransMeta;

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

  private static final String EXP_LOG_TABLE_CONN_NAME = "connName";

  private static final String EXP_LOG_TABLE_SCHEMA_NAME = "schemaName";

  private static final String EXP_LOG_TABLE_TABLE_NAME = "tableName";

  private static final String EXP_LOG_TABLE_TIMEOUT_IN_DAYS = "2";

  private static final String EXP_LOG_TABLE_SIZE_LIMIT = "250";

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

  // ~ Instance fields =================================================================================================

  protected RepositoryMeta repositoryMeta;

  protected Repository repository;

  protected UserInfo userInfo;

  // ~ Constructors ====================================================================================================

  // ~ Methods =========================================================================================================

  @Before
  public void setUp() throws Exception {
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
    repository.createRepositoryDirectory(rootDir, DIR_CLUSTERS);
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
    assertEquals("CREATE_FILE", fetchedJob.getJobEntry(0).getEntry().getTypeId());
    assertEquals(EXP_JOB_ENTRY_1_COPY_X_LOC, fetchedJob.getJobEntry(0).getLocation().x);
    assertEquals(EXP_JOB_ENTRY_1_COPY_Y_LOC, fetchedJob.getJobEntry(0).getLocation().y);
    assertEquals("DELETE_FILE", fetchedJob.getJobEntry(1).getEntry().getTypeId());
    assertEquals(EXP_JOB_ENTRY_2_COPY_X_LOC, fetchedJob.getJobEntry(1).getLocation().x);
    assertEquals(EXP_JOB_ENTRY_2_COPY_Y_LOC, fetchedJob.getJobEntry(1).getLocation().y);
    assertEquals(1, fetchedJob.getJobhops().size());
    assertEquals("CREATE_FILE", fetchedJob.getJobHop(0).getFromEntry().getEntry().getTypeId());
    assertEquals("DELETE_FILE", fetchedJob.getJobHop(0).getToEntry().getEntry().getTypeId());
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
    jobLogTable.setConnectionName(EXP_LOG_TABLE_CONN_NAME);
    jobLogTable.setLogInterval(EXP_JOB_LOG_TABLE_INTERVAL);
    jobLogTable.setSchemaName(EXP_LOG_TABLE_SCHEMA_NAME);
    jobLogTable.setLogSizeLimit(EXP_LOG_TABLE_SIZE_LIMIT);
    jobLogTable.setTableName(EXP_LOG_TABLE_TABLE_NAME);
    jobLogTable.setTimeoutInDays(EXP_LOG_TABLE_TIMEOUT_IN_DAYS);
    jobMeta.setJobLogTable(jobLogTable);
    // TODO mlowery other jobEntryLogTable fields could be set for testing here    
    JobEntryLogTable jobEntryLogTable = JobEntryLogTable.getDefault(jobMeta, jobMeta);
    jobEntryLogTable.setConnectionName(EXP_LOG_TABLE_CONN_NAME);
    jobEntryLogTable.setSchemaName(EXP_LOG_TABLE_SCHEMA_NAME);
    jobEntryLogTable.setTableName(EXP_LOG_TABLE_TABLE_NAME);
    jobEntryLogTable.setTimeoutInDays(EXP_LOG_TABLE_TIMEOUT_IN_DAYS);
    jobMeta.setJobEntryLogTable(jobEntryLogTable);
    // TODO mlowery other channelLogTable fields could be set for testing here    
    ChannelLogTable channelLogTable = ChannelLogTable.getDefault(jobMeta, jobMeta);
    channelLogTable.setConnectionName(EXP_LOG_TABLE_CONN_NAME);
    channelLogTable.setSchemaName(EXP_LOG_TABLE_SCHEMA_NAME);
    channelLogTable.setTableName(EXP_LOG_TABLE_TABLE_NAME);
    channelLogTable.setTimeoutInDays(EXP_LOG_TABLE_TIMEOUT_IN_DAYS);
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
    
    return transMeta;
  }
  
  private JobEntryInterface createJobEntry1() throws Exception {
    JobEntryCreateFile jobEntry1 = new JobEntryCreateFile(EXP_JOB_ENTRY_1_NAME);
    // how does spoon know which class to instantiate??
    jobEntry1.setFilename(EXP_JOB_ENTRY_1_FILENAME);
    return jobEntry1;
  }

  private JobEntryInterface createJobEntry2() throws Exception {
    JobEntryDeleteFile jobEntry2 = new JobEntryDeleteFile(EXP_JOB_ENTRY_2_NAME);
    // how does spoon know which class to instantiate??
    jobEntry2.setFilename(EXP_JOB_ENTRY_2_FILENAME);
    return jobEntry2;
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
  public void testGetClusterID() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetClusterIDs() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetClusterNames() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetJobEntryAttributeBooleanObjectIdString() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetJobEntryAttributeBooleanObjectIdIntString() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetJobEntryAttributeBooleanObjectIdStringBoolean() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetJobEntryAttributeIntegerObjectIdString() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetJobEntryAttributeIntegerObjectIdIntString() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetJobEntryAttributeStringObjectIdString() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetJobEntryAttributeStringObjectIdIntString() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetJobLock() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetPartitionSchemaID() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetPartitionSchemaIDs() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetPartitionSchemaNames() throws Exception {
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
  public void testLoadClusterSchema() throws Exception {
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
  public void testLoadPartitionSchema() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testLoadTransformation() throws Exception {
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
  public void testSaveJobEntryAttributeObjectIdObjectIdStringString() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testSaveJobEntryAttributeObjectIdObjectIdStringBoolean() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testSaveJobEntryAttributeObjectIdObjectIdStringLong() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testSaveJobEntryAttributeObjectIdObjectIdIntStringString() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testSaveJobEntryAttributeObjectIdObjectIdIntStringBoolean() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testSaveJobEntryAttributeObjectIdObjectIdIntStringLong() throws Exception {
    fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testSaveRepositoryDirectory() throws Exception {
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

}
