package org.pentaho.di.repository.pur;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.plugins.JobEntryPluginType;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.repository.IUser;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryTestBase;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.di.repository.pur.model.EEUserInfo;
import org.pentaho.platform.api.engine.IAuthorizationPolicy;
import org.pentaho.platform.api.engine.IPentahoSession;
import org.pentaho.platform.api.repository.IBackingRepositoryLifecycleManager;
import org.pentaho.platform.api.repository.IUnifiedRepository;
import org.pentaho.platform.api.repository.RepositoryFile;
import org.pentaho.platform.engine.core.system.PentahoSessionHolder;
import org.pentaho.platform.engine.core.system.StandaloneSession;
import org.pentaho.platform.engine.security.SecurityHelper;
import org.pentaho.test.platform.engine.core.MicroPlatform;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.security.Authentication;
import org.springframework.security.GrantedAuthority;
import org.springframework.security.GrantedAuthorityImpl;
import org.springframework.security.context.SecurityContextHolder;
import org.springframework.security.providers.UsernamePasswordAuthenticationToken;
import org.springframework.security.userdetails.User;
import org.springframework.security.userdetails.UserDetails;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.pentaho.commons.dsc.PentahoLicenseVerifier;
import com.pentaho.commons.dsc.util.TestLicenseStream;
import com.pentaho.security.policy.rolebased.IRoleAuthorizationPolicyRoleBindingDao;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/repository.spring.xml",
    "classpath:/repository-test-override.spring.xml" })
public class PurRepositoryTest extends RepositoryTestBase implements ApplicationContextAware {

  private IUnifiedRepository pur;

  private IBackingRepositoryLifecycleManager manager;

  private IRoleAuthorizationPolicyRoleBindingDao roleBindingDao;

  private static IAuthorizationPolicy authorizationPolicy;

  @BeforeClass
  public static void setUpClass() throws Exception {
    // folder cannot be deleted at teardown shutdown hooks have not yet necessarily completed
    // parent folder must match jcrRepository.homeDir bean property in repository-test-override.spring.xml
    FileUtils.deleteDirectory(new File("/tmp/jackrabbit-test"));
    PentahoSessionHolder.setStrategyName(PentahoSessionHolder.MODE_GLOBAL);

    MicroPlatform mp = new MicroPlatform();
    // used by DefaultPentahoJackrabbitAccessControlHelper
    mp.define(IAuthorizationPolicy.class, DelegatingAuthorizationPolicy.class);
    mp.start();
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    PentahoSessionHolder.removeSession();
    SecurityContextHolder.getContext().setAuthentication(null);

    // test calls into local "unified" repository which requires biserver-ee license
    PentahoLicenseVerifier.setStreamOpener(new TestLicenseStream("biserver-ee=true\npdi-ee=true")); //$NON-NLS-1$
    KettleEnvironment.init();

    // programmatically register plugins, annotation based plugins do not get loaded unless
    // they are in kettle's plugins folder.
    JobEntryPluginType.getInstance().registerCustom(JobEntryAttributeTesterJobEntry.class, "test",
        "JobEntryAttributeTester", "JobEntryAttributeTester", "JobEntryAttributeTester", "");
    StepPluginType.getInstance().registerCustom(TransStepAttributeTesterTransStep.class, "test", "StepAttributeTester",
        "StepAttributeTester", "StepAttributeTester", "");

    repositoryMeta = new PurRepositoryMeta();
    repositoryMeta.setName("JackRabbit");
    repositoryMeta.setDescription("JackRabbit test repository");
    userInfo = new UserInfo(EXP_LOGIN, "password", EXP_USERNAME, "Apache Tomcat user", true);

    repository = new PurRepository();
    repository.init(repositoryMeta);

    setUpUser();
    
    ((PurRepository) repository).setTest(pur);
    repository.connect(EXP_LOGIN, "password");
    
    List<RepositoryFile> files = pur.getChildren(pur.getFile("/public").getId());
    StringBuilder buf = new StringBuilder();
    for (RepositoryFile file : files) {
      buf.append("\n").append(file);
    }
    assertTrue("files not deleted: " + buf, files.isEmpty());
  }

  protected void loginAsTenantAdmin() {
    StandaloneSession pentahoSession = new StandaloneSession("joe");
    pentahoSession.setAuthenticated("joe");
    pentahoSession.setAttribute(IPentahoSession.TENANT_ID_KEY, "acme");
    final String password = "password";
    List<GrantedAuthority> authList = new ArrayList<GrantedAuthority>();
    authList.add(new GrantedAuthorityImpl("Authenticated"));
    authList.add(new GrantedAuthorityImpl("acme_Authenticated"));
    authList.add(new GrantedAuthorityImpl("acme_Admin"));
    GrantedAuthority[] authorities = authList.toArray(new GrantedAuthority[0]);
    UserDetails userDetails = new User("joe", password, true, true, true, true, authorities);
    Authentication auth = new UsernamePasswordAuthenticationToken(userDetails, password, authorities);
    SecurityHelper.setPrincipal(auth, pentahoSession);
    PentahoSessionHolder.setSession(pentahoSession);
    // this line necessary for Spring Security's MethodSecurityInterceptor
    SecurityContextHolder.getContext().setAuthentication(auth);
    manager.newTenant();
    manager.newUser();
  }

  protected void setUpUser() {
    StandaloneSession pentahoSession = new StandaloneSession(userInfo.getLogin());
    pentahoSession.setAuthenticated(userInfo.getLogin());
    pentahoSession.setAttribute(IPentahoSession.TENANT_ID_KEY, "acme");
    final GrantedAuthority[] authorities = new GrantedAuthority[2];
    authorities[0] = new GrantedAuthorityImpl("Authenticated");
    authorities[1] = new GrantedAuthorityImpl("acme_Authenticated");
    final String password = "ignored"; //$NON-NLS-1$
    UserDetails userDetails = new User(userInfo.getLogin(), password, true, true, true, true, authorities);
    Authentication authentication = new UsernamePasswordAuthenticationToken(userDetails, password, authorities);
    // next line is copy of SecurityHelper.setPrincipal
    pentahoSession.setAttribute("SECURITY_PRINCIPAL", authentication);
    PentahoSessionHolder.setSession(pentahoSession);
    SecurityContextHolder.setStrategyName(SecurityContextHolder.MODE_GLOBAL);
    SecurityContextHolder.getContext().setAuthentication(authentication);
    manager.newTenant();
    manager.newUser();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    try {
      // clean up after test
      List<RepositoryFile> dirs = pur.getChildren(pur.getFile("/public").getId());
      for (RepositoryFile file : dirs) {
        pur.deleteFile(file.getId(), true, null);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    repository.disconnect();
  }

  @Override
  protected void delete(ObjectId id) {
    if (id != null) {
      pur.deleteFile(id.getId(), true, null);
    }
  }

  /**
   * Tries twice to delete files. By not failing outright on the first pass, we hopefully eliminate files that are 
   * holding references to the files we cannot delete.
   */
  protected void safelyDeleteAll(final ObjectId[] ids) throws Exception {
    Exception firstException = null;

    List<String> frozenIds = new ArrayList<String>();
    for (ObjectId id : ids) {
      frozenIds.add(id.getId());
    }

    List<String> remainingIds = new ArrayList<String>();
    for (ObjectId id : ids) {
      remainingIds.add(id.getId());
    }

    try {
      for (int i = 0; i < frozenIds.size(); i++) {
        pur.deleteFile(frozenIds.get(i), true, null);
        remainingIds.remove(frozenIds.get(i));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    if (!remainingIds.isEmpty()) {

      List<String> frozenIds2 = remainingIds;

      List<String> remainingIds2 = new ArrayList<String>();
      for (String id : frozenIds2) {
        remainingIds2.add(id);
      }

      try {
        for (int i = 0; i < frozenIds2.size(); i++) {
          pur.deleteFile(frozenIds2.get(i), true, null);
          remainingIds2.remove(frozenIds2.get(i));
        }
      } catch (Exception e) {
        if (firstException == null) {
          firstException = e;
        }
      }
      if (!remainingIds2.isEmpty()) {
        throw firstException;
      }
    }
  }

  public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
    pur = (IUnifiedRepository) applicationContext.getBean("unifiedRepository");
    manager = (IBackingRepositoryLifecycleManager) applicationContext.getBean("backingRepositoryLifecycleManager");
    roleBindingDao = (IRoleAuthorizationPolicyRoleBindingDao) applicationContext
        .getBean("roleAuthorizationPolicyRoleBindingDao");
    authorizationPolicy = (IAuthorizationPolicy) applicationContext.getBean("authorizationPolicy");
    manager.startup();
  }

  @Override
  protected RepositoryDirectoryInterface loadStartDirectory() throws Exception {
    RepositoryDirectoryInterface rootDir = repository.loadRepositoryDirectoryTree();
    RepositoryDirectoryInterface startDir = rootDir.findDirectory("public");
    assertNotNull(startDir);
    return startDir;
  }

  /**
   * Allow PentahoSystem to create this class but it in turn delegates to the authorizationPolicy fetched from Spring's
   * ApplicationContext.
   */
  public static class DelegatingAuthorizationPolicy implements IAuthorizationPolicy {

    public List<String> getAllowedActions(final String actionNamespace) {
      return authorizationPolicy.getAllowedActions(actionNamespace);
    }

    public boolean isAllowed(final String actionName) {
      return authorizationPolicy.isAllowed(actionName);
    }

  }

}
