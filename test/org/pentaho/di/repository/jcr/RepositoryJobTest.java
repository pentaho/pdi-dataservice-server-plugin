package org.pentaho.di.repository.jcr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.TransientRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.tableexists.JobEntryTableExists;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.repository.ObjectRevision;
import org.pentaho.di.repository.ProfileMeta;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.RepositoryLock;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.di.repository.ProfileMeta.Permission;

import com.pentaho.commons.dsc.PentahoLicenseVerifier;
import com.pentaho.commons.dsc.util.TestLicenseStream;

@Ignore
public class RepositoryJobTest {
	
	private JCRRepositoryMeta repositoryMeta;
	private JCRRepository repository;
	private UserInfo userInfo;
	private JobMeta	jobMeta;
	private RepositoryDirectory	directoryTree;
	
	private static final String TEST_DIRECTORY_PATH = "/test-jobs/";
	private static final String TEST_DIRECTORY_PATH_OK = "/test-jobs";
	
	private static final String TEST_JOB = "./testfiles/TestJob.kjb";
	private static final String	JOB_NAME = "Test Job";
	
	private static final String DESCRIPTION_ONE   = "Description 1";
	private static final String DESCRIPTION_TWO   = "Description 2";
	private static final String DESCRIPTION_THREE = "Description 3";
	private static final String DESCRIPTION_FOUR  = "Description 4";
	
	private static final String REVISION_COMMENT_ONE = "This is a first test version comment";
	private static final String VERSION_COMMENT_TWO = "This is a second test version comment";
	private static final String VERSION_COMMENT_THREE = "This is a third test version comment";
	private static final String VERSION_COMMENT_FOUR = "This is a fourth test version comment";
	
  @Before
	public void setUp() throws Exception {
		
    PentahoLicenseVerifier.setStreamOpener( new TestLicenseStream( "pdi-ee=true" ) ); //$NON-NLS-1$

		
		KettleEnvironment.init();
	
		repositoryMeta = new JCRRepositoryMeta();
		repositoryMeta.setName("JackRabbit");
		repositoryMeta.setDescription("JackRabbit test repository");
//		repositoryMeta.setRepositoryLocation(new JCRRepositoryLocation("http://localhost:8080/jackrabbit/rmi"));
		
		ProfileMeta adminProfile = new ProfileMeta("admin", "Administrator");
		adminProfile.addPermission(Permission.ADMIN);
		
		userInfo = new UserInfo("joe", "password", "Apache Tomcat", "Apache Tomcat user", true, adminProfile);
		
		repository = new JCRRepository();
		
    File repoDir = new File("/tmp/pdi_jcr_repo_unit_test");
    FileUtils.deleteDirectory(repoDir);
    assertTrue(repoDir.mkdir());
    javax.jcr.Repository jcrRepository = new TransientRepository(repoDir);
    ((JCRRepository) repository).setJcrRepository(jcrRepository);
		
		repository.init(repositoryMeta, userInfo);
		
		repository.connect();
		
		directoryTree = repository.loadRepositoryDirectoryTree();
		
		if (jobMeta==null) {
			jobMeta = new JobMeta(TEST_JOB, repository);
			RepositoryDirectory directory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);
			if (directory!=null) {
				jobMeta.setRepositoryDirectory(directory);
			}
			jobMeta.setDescription(DESCRIPTION_ONE);
		}
	}
	
  @After
	public void tearDown() throws Exception {  
		repository.disconnect();
    FileUtils.deleteDirectory(new File("/tmp/pdi_jcr_repo_unit_test"));
	}
  
  @Test
	public void test01_createDirectory() throws Exception {
		RepositoryDirectory tree = repository.loadRepositoryDirectoryTree();
		RepositoryDirectory fooDirectory = tree.findDirectory(TEST_DIRECTORY_PATH);
		
		if (fooDirectory==null) {
			fooDirectory = repository.createRepositoryDirectory(tree, TEST_DIRECTORY_PATH);
		}
		
		assertNotNull(fooDirectory);
		assertNotNull(fooDirectory.getObjectId());
		assertEquals(fooDirectory.getPath(), TEST_DIRECTORY_PATH_OK);
	}
  
  @Test
	public void test10_saveJob() throws Exception {
		
		// Save the job first...
		//
		repository.save(jobMeta, REVISION_COMMENT_ONE, null);
		
		assertNotNull("Object ID needs to be set", jobMeta.getObjectId());
		
		ObjectRevision version = jobMeta.getObjectRevision();
		
		assertNotNull("Object version needs to be set", version);
		
		assertEquals(REVISION_COMMENT_ONE, version.getComment());
		
		assertEquals("1.0", version.getName());
	}
  
  @Test
	public void test15_loadJob() throws Exception {

		RepositoryDirectory fooDirectory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);

		JobMeta jobMeta = repository.loadJob(JOB_NAME, fooDirectory, null, null);  // Load the last version

		assertNotNull(jobMeta);
		
		// Verify general content
		//
		ObjectRevision version = jobMeta.getObjectRevision();
		assertNotNull("Object version needs to be set", version);
		assertEquals(REVISION_COMMENT_ONE, version.getComment());
		assertEquals("1.0", version.getName());
		assertEquals("joe", version.getLogin());
		
		// Verify specific content
		//
		assertEquals(6, jobMeta.nrJobEntries());
		assertEquals(6, jobMeta.nrJobHops());

		JobEntryCopy entry = jobMeta.findJobEntry("Table exists 1");
		JobEntryTableExists meta = (JobEntryTableExists) entry.getEntry();
		DatabaseMeta databaseMeta = meta.getDatabase();
		assertNotNull(databaseMeta);
	}
  
  @Test
	public void test20_createJobRevisions() throws Exception {
	
		// Change the description of the job & save it again..
		//
		jobMeta.setDescription(DESCRIPTION_TWO);
		repository.save(jobMeta, VERSION_COMMENT_TWO, null);
		
		String id = jobMeta.getObjectId().getId();
		assertEquals("1.1", jobMeta.getObjectRevision().getName());
	
		// Change the description of the job & save it again..
		//
		jobMeta.setDescription(DESCRIPTION_THREE);
		JobEntryCopy populate = jobMeta.findJobEntry("Populate");
		assertNotNull(populate);
		populate.setName("NEW_NAME");
		repository.save(jobMeta, VERSION_COMMENT_THREE, null);

		assertEquals("1.2", jobMeta.getObjectRevision().getName());
		assertEquals(id, jobMeta.getObjectId().getId());

		// Change the description of the job & save it again..
		//
		jobMeta.setDescription(DESCRIPTION_FOUR);
		jobMeta.removeJobHop(jobMeta.nrJobHops()-1);
		jobMeta.removeJobEntry(jobMeta.nrJobEntries()-1);
		repository.save(jobMeta, VERSION_COMMENT_FOUR, null);
		assertEquals(VERSION_COMMENT_FOUR, jobMeta.getObjectRevision().getComment());
		assertEquals("1.3", jobMeta.getObjectRevision().getName());
		assertEquals(id, jobMeta.getObjectId().getId());
	}
  
  @Test
	public void test30_getJobRevisionHistory() throws Exception {
		
		List<ObjectRevision> versions = repository.getRevisions(jobMeta);
		assertEquals(versions.size(), 4);
		
		ObjectRevision v1 = versions.get(0);
		assertEquals("1.0", v1.getName());
		assertEquals(REVISION_COMMENT_ONE, v1.getComment());
		
		ObjectRevision v2 = versions.get(1);
		assertEquals("1.1", v2.getName());
		assertEquals(VERSION_COMMENT_TWO, v2.getComment());

		ObjectRevision v3 = versions.get(2);
		assertEquals("1.2", v3.getName());
		assertEquals(VERSION_COMMENT_THREE, v3.getComment());

		ObjectRevision v4 = versions.get(3);
		assertEquals("1.3", v4.getName());
		assertEquals(VERSION_COMMENT_FOUR, v4.getComment());
	}
  
  @Test
	public void test40_loadJobRevisions() throws Exception {

		RepositoryDirectory fooDirectory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);

		JobMeta jobMeta = repository.loadJob(JOB_NAME, fooDirectory, null, "1.1");  // Load the second version
		ObjectRevision version = jobMeta.getObjectRevision();
		assertEquals("1.1", version.getName());
		assertEquals(VERSION_COMMENT_TWO, version.getComment());
		assertEquals(DESCRIPTION_TWO, jobMeta.getDescription());
		assertEquals(6, jobMeta.nrJobEntries());
		assertEquals(6, jobMeta.nrJobHops());
		
		jobMeta = repository.loadJob(JOB_NAME, fooDirectory, null, "1.0");  // Load the first version
		version = jobMeta.getObjectRevision();
		assertEquals("1.0", version.getName());
		assertEquals(REVISION_COMMENT_ONE, version.getComment());
		assertEquals(DESCRIPTION_ONE, jobMeta.getDescription());
		assertEquals(6, jobMeta.nrJobEntries());
		assertEquals(6, jobMeta.nrJobHops());

		jobMeta = repository.loadJob(JOB_NAME, fooDirectory, null, "1.3");  // Load the fourth version
		version = jobMeta.getObjectRevision();
		assertEquals("1.3", version.getName());
		assertEquals(VERSION_COMMENT_FOUR, version.getComment());
		assertEquals(DESCRIPTION_FOUR, jobMeta.getDescription());
		assertEquals(5, jobMeta.nrJobEntries());
		assertEquals(5, jobMeta.nrJobHops());

		jobMeta = repository.loadJob(JOB_NAME, fooDirectory, null, "1.2");  // Load the third version
		version = jobMeta.getObjectRevision();
		assertEquals("1.2", version.getName());
		assertEquals(VERSION_COMMENT_THREE, version.getComment());
		assertEquals(DESCRIPTION_THREE, jobMeta.getDescription());
		assertEquals(6, jobMeta.nrJobEntries());
		assertEquals(6, jobMeta.nrJobHops());
		assertNotNull(jobMeta.findJobEntry("NEW_NAME"));
	}
  
  @Test
	public void test50_loadLastJobRevision() throws Exception {
		
		RepositoryDirectory fooDirectory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);
		jobMeta = repository.loadJob(JOB_NAME, fooDirectory, null, null);  // Load the last version
		
		assertNotNull(jobMeta);
		
		ObjectRevision version = jobMeta.getObjectRevision();
		
		assertEquals(VERSION_COMMENT_FOUR, version.getComment());
		assertEquals("1.3", version.getName());
	}
  
  @Test
	public void test60_lockJob() throws Exception {
		RepositoryDirectory fooDirectory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);
		jobMeta = repository.loadJob(JOB_NAME, fooDirectory, null, null);  // Load the last version
		repository.lockJob(jobMeta.getObjectId(), "Locked by unit test");
		
		RepositoryLock lock = repository.getJobLock(jobMeta.getObjectId());
		
		assertNotNull(lock);
	}
  
  @Test
	public void test65_unlockJob() throws Exception {
		RepositoryDirectory fooDirectory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);
		jobMeta = repository.loadJob(JOB_NAME, fooDirectory, null, null);  // Load the last version
		repository.unlockJob(jobMeta.getObjectId());
	}
  
  @Test
	public void test70_existsJob() throws Exception {
		RepositoryDirectory fooDirectory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);
		jobMeta = repository.loadJob(JOB_NAME, fooDirectory, null, null);  // Load the last version
		
		boolean exists = repository.exists(JOB_NAME, fooDirectory, RepositoryObjectType.JOB);
		
		assertEquals("Job exists in the repository, test didn't find it", true, exists);
	}
/*
	public void test75_deleteJob() throws Exception {
		RepositoryDirectory fooDirectory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);
		jobMeta = repository.loadJob(JOB_NAME, fooDirectory, null, null);  // Load the last version
		
		repository.deleteJob(jobMeta.getObjectId());

		boolean exists = repository.exists(JOB_NAME, fooDirectory, RepositoryObjectType.JOB);
		assertEquals("Job was not deleted", false, exists);
	}

	public void test77_restoreJob() throws Exception {
		RepositoryDirectory fooDirectory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);
		jobMeta = repository.loadJob(JOB_NAME, fooDirectory, null, null);  // Load the last version
		
		repository.undeleteObject(jobMeta); // un-delete

		boolean exists = repository.exists(JOB_NAME, fooDirectory, RepositoryObjectType.JOB);
		assertEquals("Job was not restored", true, exists);
		
		repository.undeleteObject(jobMeta); // restore the second version...
		
		jobMeta = repository.loadJob(JOB_NAME, fooDirectory, null, null);  // Load the last version
		
		ObjectRevision version = jobMeta.getObjectRevision();
		assertEquals("1.5", version.getName());
	}

	public void test77_renameDatabase() throws Exception {
		RepositoryDirectory fooDirectory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);
		jobMeta = repository.loadJob(JOB_NAME, fooDirectory, null, null);  // Load the last version
		
		// Rename connection "H2 test", used in the job...
		//
		ObjectId id = repository.getDatabaseID("H2 test");
		assertNotNull(id);
		
		repository.renameDatabase(id, "new H2");

		jobMeta = repository.loadJob(JOB_NAME, fooDirectory, null, null);  // Load the last version
		JobEntryCopy copy = jobMeta.findJobEntry("Table exists 1");
		JobEntryTableExists meta = (JobEntryTableExists) copy.getEntry();
		
		DatabaseMeta databaseMeta = meta.getDatabase();
		assertNotNull(databaseMeta);
		assertEquals("new H2", databaseMeta.getName());
		
		DatabaseMeta logDatabase = jobMeta.getJobLogTable().getDatabaseMeta();
		assertNotNull(logDatabase);
		assertEquals("new H2", logDatabase.getName());
	}
	
	public void test99_deleteDirectory() throws Exception {
		RepositoryDirectory fooDirectory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);
		assertNotNull(fooDirectory);
		assertNotNull(fooDirectory.getObjectId());
		
		repository.deleteRepositoryDirectory(fooDirectory);
		directoryTree = repository.loadRepositoryDirectoryTree();
		fooDirectory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);
		assertNull(fooDirectory);
	}
	*/
}
