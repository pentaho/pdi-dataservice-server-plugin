package org.pentaho.di.repository.jcr;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.TransientRepository;
import org.junit.After;
import org.junit.Before;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.repository.ProfileMeta;
import org.pentaho.di.repository.AbstractRepositoryTest;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.di.repository.ProfileMeta.Permission;

import com.pentaho.commons.dsc.PentahoLicenseVerifier;
import com.pentaho.commons.dsc.util.TestLicenseStream;

public class JCRRepositoryTest extends AbstractRepositoryTest {

  @Before
  public void setUp() throws Exception {
    // tell kettle to look for plugins in this package (because custom plugins are defined in this class)
    System.setProperty(Const.KETTLE_PLUGIN_PACKAGES, AbstractRepositoryTest.class.getPackage().getName());

    PentahoLicenseVerifier.setStreamOpener(new TestLicenseStream("pdi-ee=true")); //$NON-NLS-1$
    KettleEnvironment.init();
    repositoryMeta = new JCRRepositoryMeta();
    repositoryMeta.setName("JackRabbit");
    repositoryMeta.setDescription("JackRabbit test repository");
    ((JCRRepositoryMeta) repositoryMeta).setRepositoryLocation(new JCRRepositoryLocation(
        "http://localhost:8080/jackrabbit/rmi"));
    ProfileMeta adminProfile = new ProfileMeta("admin", "Administrator");
    adminProfile.addPermission(Permission.ADMIN);
    userInfo = new UserInfo(EXP_LOGIN, "password", EXP_USERNAME, "Apache Tomcat user", true, adminProfile);
    repository = new JCRRepository();
    File repoDir = new File("/tmp/pdi_jcr_repo_unit_test");
    FileUtils.deleteDirectory(repoDir);
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

}
