package org.pentaho.di.repository.jcr;

import java.util.List;

import junit.framework.TestCase;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.ObjectVersion;
import org.pentaho.di.repository.ProfileMeta;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.RepositoryLock;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.di.repository.ProfileMeta.Permission;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;

public class RepositoryTransformationTests extends TestCase {
	
	private JCRRepositoryMeta repositoryMeta;
	private JCRRepository repository;
	private UserInfo userInfo;
	private TransMeta	transMeta;
	private RepositoryDirectory	directoryTree;
	
	private static final String TEST_DIRECTORY_PATH = "/foo/bar/test/dir";
	
	private static final String TEST_TRANSFORMATION = "./testfiles/SunTest.ktr";
	
	private static final String DESCRIPTION_ONE   = "Description 1";
	private static final String DESCRIPTION_TWO   = "Description 2";
	private static final String DESCRIPTION_THREE = "Description 3";
	private static final String DESCRIPTION_FOUR  = "Description 4";
	
	private static final String VERSION_COMMENT_ONE = "This is a first test version comment";
	private static final String VERSION_COMMENT_TWO = "This is a second test version comment";
	private static final String VERSION_COMMENT_THREE = "This is a third test version comment";
	private static final String VERSION_COMMENT_FOUR = "This is a fourth test version comment";
	
	protected void setUp() throws Exception {
		super.setUp();
		
		KettleEnvironment.init();
	
		repositoryMeta = new JCRRepositoryMeta();
		repositoryMeta.setName("JackRabbit");
		repositoryMeta.setDescription("JackRabbit test repository");
		repositoryMeta.setRepositoryLocation(new JCRRepositoryLocation("http://localhost:8181/jackrabbit/rmi"));
		
		ProfileMeta adminProfile = new ProfileMeta("admin", "Administrator");
		adminProfile.addPermission(Permission.ADMIN);
		
		userInfo = new UserInfo("tomcat", "tomcat", "Apache Tomcat", "Apache Tomcat user", true, adminProfile);
		
		repository = new JCRRepository();
		repository.init(repositoryMeta, userInfo);
		
		repository.connect();
		
		directoryTree = repository.loadRepositoryDirectoryTree();
		
		if (transMeta==null) {
			transMeta = new TransMeta(TEST_TRANSFORMATION);
			RepositoryDirectory directory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);
			if (directory!=null) {
				transMeta.setRepositoryDirectory(directory);
			}
			transMeta.setDescription(DESCRIPTION_ONE);
		}
	}
	
	protected void tearDown() throws Exception {
		repository.disconnect();
		super.tearDown();
	}
	
	public void test01_createDirectory() throws Exception {
		RepositoryDirectory tree = repository.loadRepositoryDirectoryTree();
		RepositoryDirectory fooDirectory = tree.findDirectory(TEST_DIRECTORY_PATH);
		
		if (fooDirectory==null) {
			fooDirectory = repository.createRepositoryDirectory(tree, TEST_DIRECTORY_PATH);
		}
		
		assertNotNull(fooDirectory);
		assertNotNull(fooDirectory.getObjectId());
		assertEquals(fooDirectory.getPath(), TEST_DIRECTORY_PATH);
	}
	
	public void test10_saveTransformations() throws Exception {
		
		// Save the transformation first...
		//
		repository.save(transMeta, VERSION_COMMENT_ONE, null);
		
		assertNotNull("Object ID needs to be set", transMeta.getObjectId());
		
		ObjectVersion version = transMeta.getObjectVersion();
		
		assertNotNull("Object version needs to be set", version);
		
		assertEquals(VERSION_COMMENT_ONE, version.getComment());
		
		assertEquals("1.0", version.getName());
	}
	
	public void test15_loadTransformaion() throws Exception {

		RepositoryDirectory fooDirectory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);

		TransMeta transMeta = repository.loadTransformation("SunTest", fooDirectory, null, true, null);  // Load the last version

		assertNotNull(transMeta);
		
		// Verify general content
		//
		ObjectVersion version = transMeta.getObjectVersion();
		assertNotNull("Object version needs to be set", version);
		assertEquals(VERSION_COMMENT_ONE, version.getComment());
		assertEquals("1.0", version.getName());
		assertEquals("tomcat", version.getLogin());
		
		// Verify specific content
		//
		assertEquals(3, transMeta.nrSteps());
		assertEquals(2, transMeta.nrTransHops());

		StepMeta step = transMeta.findStep("Table Output");
		TableOutputMeta meta = (TableOutputMeta) step.getStepMetaInterface();
		DatabaseMeta databaseMeta = meta.getDatabaseMeta();
		assertNotNull(databaseMeta);

	}
	
	public void test20_createTransformationVersions() throws Exception {
	
		// Change the description of the transformation & Save the transformation again..
		//
		transMeta.setDescription(DESCRIPTION_TWO);
		repository.save(transMeta, VERSION_COMMENT_TWO, null);
		
		String id = transMeta.getObjectId().getId();
		assertEquals("1.1", transMeta.getObjectVersion().getName());
	
		// Change the description of the transformation & Save the transformation again..
		//
		transMeta.setDescription(DESCRIPTION_THREE);
		transMeta.getStep(0).setName("NEW_NAME");
		repository.save(transMeta, VERSION_COMMENT_THREE, null);

		assertEquals("1.2", transMeta.getObjectVersion().getName());
		assertEquals(id, transMeta.getObjectId().getId());

		// Change the description of the transformation & Save the transformation again..
		//
		transMeta.setDescription(DESCRIPTION_FOUR);
		transMeta.removeTransHop(1);
		transMeta.removeStep(2);
		repository.save(transMeta, VERSION_COMMENT_FOUR, null);
		assertEquals(VERSION_COMMENT_FOUR, transMeta.getObjectVersion().getComment());
		assertEquals("1.3", transMeta.getObjectVersion().getName());
		assertEquals(id, transMeta.getObjectId().getId());
	}
	
	public void test30_getTransformationVersionHistory() throws Exception {
		
		List<ObjectVersion> versions = repository.getVersions(transMeta);
		assertEquals(versions.size(), 4);
		
		ObjectVersion v1 = versions.get(0);
		assertEquals("1.0", v1.getName());
		assertEquals(VERSION_COMMENT_ONE, v1.getComment());
		
		ObjectVersion v2 = versions.get(1);
		assertEquals("1.1", v2.getName());
		assertEquals(VERSION_COMMENT_TWO, v2.getComment());

		ObjectVersion v3 = versions.get(2);
		assertEquals("1.2", v3.getName());
		assertEquals(VERSION_COMMENT_THREE, v3.getComment());

		ObjectVersion v4 = versions.get(3);
		assertEquals("1.3", v4.getName());
		assertEquals(VERSION_COMMENT_FOUR, v4.getComment());
	}
	
	public void test40_loadTransformationVersions() throws Exception {

		RepositoryDirectory fooDirectory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);

		TransMeta transMeta = repository.loadTransformation("SunTest", fooDirectory, null, true, "1.1");  // Load the second version
		ObjectVersion version = transMeta.getObjectVersion();
		assertEquals("1.1", version.getName());
		assertEquals(VERSION_COMMENT_TWO, version.getComment());
		assertEquals(DESCRIPTION_TWO, transMeta.getDescription());
		assertEquals(3, transMeta.nrSteps());
		assertEquals(2, transMeta.nrTransHops());
		
		transMeta = repository.loadTransformation("SunTest", fooDirectory, null, true, "1.0");  // Load the first version
		version = transMeta.getObjectVersion();
		assertEquals("1.0", version.getName());
		assertEquals(VERSION_COMMENT_ONE, version.getComment());
		assertEquals(DESCRIPTION_ONE, transMeta.getDescription());
		assertEquals(3, transMeta.nrSteps());
		assertEquals(2, transMeta.nrTransHops());

		transMeta = repository.loadTransformation("SunTest", fooDirectory, null, true, "1.3");  // Load the fourth version
		version = transMeta.getObjectVersion();
		assertEquals("1.3", version.getName());
		assertEquals(VERSION_COMMENT_FOUR, version.getComment());
		assertEquals(DESCRIPTION_FOUR, transMeta.getDescription());
		assertEquals(2, transMeta.nrSteps());
		assertEquals(1, transMeta.nrTransHops());

		transMeta = repository.loadTransformation("SunTest", fooDirectory, null, true, "1.2");  // Load the third version
		version = transMeta.getObjectVersion();
		assertEquals("1.2", version.getName());
		assertEquals(VERSION_COMMENT_THREE, version.getComment());
		assertEquals(DESCRIPTION_THREE, transMeta.getDescription());
		assertEquals(3, transMeta.nrSteps());
		assertEquals(2, transMeta.nrTransHops());
		assertEquals("NEW_NAME", transMeta.getStep(0).getName());
	}

	public void test50_loadLastTransformationVersion() throws Exception {
		
		RepositoryDirectory fooDirectory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);
		transMeta = repository.loadTransformation("SunTest", fooDirectory, null, true, null);  // Load the last version
		
		assertNotNull(transMeta);
		
		ObjectVersion version = transMeta.getObjectVersion();
		
		assertEquals(VERSION_COMMENT_FOUR, version.getComment());
		assertEquals("1.3", version.getName());
	}
	
	public void test60_lockTransformation() throws Exception {
		RepositoryDirectory fooDirectory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);
		transMeta = repository.loadTransformation("SunTest", fooDirectory, null, true, null);  // Load the last version
		repository.lockTransformation(transMeta.getObjectId(), "Locked by unit test");
		
		RepositoryLock lock = repository.getTransformationLock(transMeta.getObjectId());
		
		assertNotNull(lock);
	}
	
	public void test65_unlockTransformation() throws Exception {
		RepositoryDirectory fooDirectory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);
		transMeta = repository.loadTransformation("SunTest", fooDirectory, null, true, null);  // Load the last version
		repository.unlockTransformation(transMeta.getObjectId());
	}

	public void test70_existsTransformation() throws Exception {
		RepositoryDirectory fooDirectory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);
		transMeta = repository.loadTransformation("SunTest", fooDirectory, null, true, null);  // Load the last version
		
		// ObjectId id = repository.getTransformationID(transMeta.getName(), transMeta.getRepositoryDirectory());
		boolean exists = repository.exists(transMeta);
		
		assertEquals("Transformation exists in the repository, test didn't find it", true, exists);
	}

	public void test75_deleteTransformation() throws Exception {
		RepositoryDirectory fooDirectory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);
		transMeta = repository.loadTransformation("SunTest", fooDirectory, null, true, null);  // Load the last version
		
		repository.deleteTransformation(transMeta.getObjectId());

		boolean exists = repository.exists(transMeta);
		assertEquals("Transformation was not deleted", false, exists);
	}

	public void test77_restoreTransformation() throws Exception {
		RepositoryDirectory fooDirectory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);
		transMeta = repository.loadTransformation("SunTest", fooDirectory, null, true, null);  // Load the last version
		
		repository.undeleteObject(transMeta); // un-delete

		boolean exists = repository.exists(transMeta);
		assertEquals("Transformation was not restored", true, exists);
		
		repository.undeleteObject(transMeta); // restore the second version...
		
		transMeta = repository.loadTransformation("SunTest", fooDirectory, null, true, null);  // Load the last version
		
		ObjectVersion version = transMeta.getObjectVersion();
		assertEquals("1.5", version.getName());
	}

	public void test77_renameDatabase() throws Exception {
		RepositoryDirectory fooDirectory = directoryTree.findDirectory(TEST_DIRECTORY_PATH);
		transMeta = repository.loadTransformation("SunTest", fooDirectory, null, true, null);  // Load the last version
		
		// Rename connection "MySQL", used in the transformation...
		//
		ObjectId id = repository.getDatabaseID("MySQL");
		assertNotNull(id);
		
		repository.renameDatabase(id, "new MySQL");

		transMeta = repository.loadTransformation("SunTest", fooDirectory, null, true, null);  // Load the last version
		StepMeta step = transMeta.findStep("Table Output");
		TableOutputMeta meta = (TableOutputMeta) step.getStepMetaInterface();
		DatabaseMeta databaseMeta = meta.getDatabaseMeta();
		assertNotNull(databaseMeta);
		
		assertEquals("new MySQL", databaseMeta.getName());
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
}
