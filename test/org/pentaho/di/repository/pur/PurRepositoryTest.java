package org.pentaho.di.repository.pur;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.repository.AbstractRepositoryTest;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.ProfileMeta;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.RepositoryObject;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.di.repository.ProfileMeta.Permission;
import org.pentaho.platform.api.repository.IRepositoryService;
import org.pentaho.platform.api.repository.RepositoryFile;
import org.pentaho.platform.engine.core.system.PentahoSessionHolder;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.security.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.pentaho.commons.dsc.PentahoLicenseVerifier;
import com.pentaho.commons.dsc.util.TestLicenseStream;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/sample-repository.spring.xml",
    "classpath:/sample-repository-test-override.spring.xml" })
public class PurRepositoryTest extends AbstractRepositoryTest implements ApplicationContextAware {

  private IRepositoryService pur;

  @BeforeClass
  public static void setUpClass() throws Exception {
    // folder cannot be deleted at teardown shutdown hooks have not yet necessarily completed
    // parent folder must match jcrRepository.homeDir bean property in repository-test-override.spring.xml
    FileUtils.deleteDirectory(new File("/tmp/jackrabbit-test"));
    PentahoSessionHolder.setStrategyName(PentahoSessionHolder.MODE_GLOBAL);
  }

  @Before
  public void setUp() throws Exception {
    PentahoSessionHolder.removeSession();
    SecurityContextHolder.getContext().setAuthentication(null);

    // tell kettle to look for plugins in this package (because custom plugins are defined in this class)
    System.setProperty(Const.KETTLE_PLUGIN_PACKAGES, AbstractRepositoryTest.class.getPackage().getName());

    PentahoLicenseVerifier.setStreamOpener(new TestLicenseStream("pdi-ee=true")); //$NON-NLS-1$
    KettleEnvironment.init();

    repositoryMeta = new PurRepositoryMeta();
    repositoryMeta.setName("JackRabbit");
    repositoryMeta.setDescription("JackRabbit test repository");
    ProfileMeta adminProfile = new ProfileMeta("admin", "Administrator");
    adminProfile.addPermission(Permission.ADMIN);
    userInfo = new UserInfo(EXP_LOGIN, "password", EXP_USERNAME, "Apache Tomcat user", true, adminProfile);

    repository = new PurRepository();
    ((PurRepository) repository).setPur(pur);

    repository.init(repositoryMeta, userInfo);
    repository.connect();

    List<RepositoryFile> files = pur.getChildren(pur.getFile("/pentaho/acme/public").getId());
    assertTrue("files not deleted: " + files.toString(), files.isEmpty());
  }

  @After
  public void tearDown() throws Exception {

    try {
      // clean up after test
      // delete in correct order to prevent ref integrity exceptions
      // transformations
      RepositoryFile transParentFolder = pur.getFile("/pentaho/acme/public/transformations");
      if (transParentFolder != null) {
        List<RepositoryObject> files = repository.getTransformationObjects(new StringObjectId(transParentFolder.getId()
            .toString()), true);
        for (RepositoryObject file : files) {
          pur.permanentlyDeleteFile(file.getObjectId().getId());
        }
      }
      // databases
      ObjectId[] dbIds = repository.getDatabaseIDs(true);

      for (ObjectId dbId : dbIds) {
        pur.permanentlyDeleteFile(dbId.getId());
      }
      // dirs
      List<RepositoryFile> dirs = pur.getChildren(pur.getFile("/pentaho/acme/public").getId());
      for (RepositoryFile file : dirs) {
        pur.permanentlyDeleteFile(file.getId());
      }

    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }

    repository.disconnect();

    //    repositoryMeta = null;
    //    repository = null;
    //    userInfo = null;
    //    FileUtils.deleteDirectory(new File("/tmp/pdi_jcr_repo_unit_test"));
  }

  public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
    pur = (IRepositoryService) applicationContext.getBean("repositoryService");
    pur.getRepositoryEventHandler().onStartup();
  }

  @Override
  protected RepositoryDirectory loadStartDirectory() throws Exception {
    RepositoryDirectory rootDir = repository.loadRepositoryDirectoryTree();
    RepositoryDirectory startDir = rootDir.findDirectory("pentaho/acme/public");
    assertNotNull(startDir);
    return startDir;
  }
}
