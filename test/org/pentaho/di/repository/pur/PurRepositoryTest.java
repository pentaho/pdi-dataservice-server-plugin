package org.pentaho.di.repository.pur;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs.FileObject;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pentaho.di.cluster.ClusterSchema;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.ProgressMonitorListener;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.plugins.JobEntryPluginType;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.partition.PartitionSchema;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.ObjectRevision;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.repository.RepositoryTestBase;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.platform.api.engine.IAuthorizationPolicy;
import org.pentaho.platform.api.engine.IPentahoSession;
import org.pentaho.platform.api.repository2.unified.IBackingRepositoryLifecycleManager;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.engine.core.system.PentahoSessionHolder;
import org.pentaho.platform.engine.core.system.StandaloneSession;
import org.pentaho.platform.engine.security.SecurityHelper;
import org.pentaho.platform.security.policy.rolebased.IRoleAuthorizationPolicyRoleBindingDao;
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
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.ext.DefaultHandler2;

import com.pentaho.commons.dsc.PentahoLicenseVerifier;
import com.pentaho.commons.dsc.util.TestLicenseStream;

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

  @Test
  public void testLoadSharedObjects_databases() throws Exception {
    PurRepository repo = (PurRepository) repository;
    DatabaseMeta dbMeta = createDatabaseMeta(EXP_DBMETA_NAME);
    repository.save(dbMeta, VERSION_COMMENT_V1, null);

    Map<RepositoryObjectType, List<? extends SharedObjectInterface>> sharedObjectsByType = new HashMap<RepositoryObjectType, List<? extends SharedObjectInterface>>();
    repo.readSharedObjects(sharedObjectsByType, RepositoryObjectType.DATABASE);
    assertNotNull(sharedObjectsByType);
    @SuppressWarnings("unchecked")
    List<DatabaseMeta> databaseMetas = (List<DatabaseMeta>) sharedObjectsByType.get(RepositoryObjectType.DATABASE);
    assertNotNull(databaseMetas);
    assertEquals(1, databaseMetas.size());
    DatabaseMeta dbMetaResult = databaseMetas.get(0);
    assertNotNull(dbMetaResult);
    assertEquals(dbMeta, dbMetaResult);

    repository.deleteDatabaseMeta(EXP_DBMETA_NAME);
  }

  @Test
  public void testLoadSharedObjects_slaves() throws Exception {
    PurRepository repo = (PurRepository) repository;
    SlaveServer slave = createSlaveServer(""); //$NON-NLS-1$
    repository.save(slave, VERSION_COMMENT_V1, null);

    Map<RepositoryObjectType, List<? extends SharedObjectInterface>> sharedObjectsByType = new HashMap<RepositoryObjectType, List<? extends SharedObjectInterface>>();
    repo.readSharedObjects(sharedObjectsByType, RepositoryObjectType.SLAVE_SERVER);
    assertNotNull(sharedObjectsByType);
    @SuppressWarnings("unchecked")
    List<SlaveServer> slaveServers = (List<SlaveServer>) sharedObjectsByType.get(RepositoryObjectType.SLAVE_SERVER);
    assertNotNull(slaveServers);
    assertEquals(1, slaveServers.size());
    SlaveServer slaveResult = slaveServers.get(0);
    assertNotNull(slaveResult);
    assertEquals(slave, slaveResult);

    repository.deleteSlave(slave.getObjectId());
  }

  @Test
  public void testLoadSharedObjects_partitions() throws Exception {
    PurRepository repo = (PurRepository) repository;
    PartitionSchema partSchema = createPartitionSchema(""); //$NON-NLS-1$
    repository.save(partSchema, VERSION_COMMENT_V1, null);

    Map<RepositoryObjectType, List<? extends SharedObjectInterface>> sharedObjectsByType = new HashMap<RepositoryObjectType, List<? extends SharedObjectInterface>>();
    repo.readSharedObjects(sharedObjectsByType, RepositoryObjectType.PARTITION_SCHEMA);
    assertNotNull(sharedObjectsByType);
    @SuppressWarnings("unchecked")
    List<PartitionSchema> partitionSchemas = (List<PartitionSchema>) sharedObjectsByType
        .get(RepositoryObjectType.PARTITION_SCHEMA);
    assertNotNull(partitionSchemas);
    assertEquals(1, partitionSchemas.size());
    PartitionSchema partitionSchemaResult = partitionSchemas.get(0);
    assertNotNull(partitionSchemaResult);
    assertEquals(partSchema, partitionSchemaResult);

    repository.deletePartitionSchema(partSchema.getObjectId());
  }

  @Test
  public void testLoadSharedObjects_clusters() throws Exception {
    PurRepository repo = (PurRepository) repository;
    ClusterSchema clusterSchema = createClusterSchema(EXP_CLUSTER_SCHEMA_NAME);
    repository.save(clusterSchema, VERSION_COMMENT_V1, null);

    Map<RepositoryObjectType, List<? extends SharedObjectInterface>> sharedObjectsByType = new HashMap<RepositoryObjectType, List<? extends SharedObjectInterface>>();
    repo.readSharedObjects(sharedObjectsByType, RepositoryObjectType.CLUSTER_SCHEMA);
    assertNotNull(sharedObjectsByType);
    @SuppressWarnings("unchecked")
    List<ClusterSchema> clusterSchemas = (List<ClusterSchema>) sharedObjectsByType
        .get(RepositoryObjectType.CLUSTER_SCHEMA);
    assertNotNull(clusterSchemas);
    assertEquals(1, clusterSchemas.size());
    ClusterSchema clusterSchemaResult = clusterSchemas.get(0);
    assertNotNull(clusterSchemaResult);
    assertEquals(clusterSchema, clusterSchemaResult);

    repository.deleteClusterSchema(clusterSchema.getObjectId());
  }

  @Test
  public void testLoadSharedObjects_databases_and_clusters() throws Exception {
    PurRepository repo = (PurRepository) repository;
    DatabaseMeta dbMeta = createDatabaseMeta(EXP_DBMETA_NAME);
    repository.save(dbMeta, VERSION_COMMENT_V1, null);
    ClusterSchema clusterSchema = createClusterSchema(EXP_CLUSTER_SCHEMA_NAME);
    repository.save(clusterSchema, VERSION_COMMENT_V1, null);

    Map<RepositoryObjectType, List<? extends SharedObjectInterface>> sharedObjectsByType = new HashMap<RepositoryObjectType, List<? extends SharedObjectInterface>>();
    repo.readSharedObjects(sharedObjectsByType, RepositoryObjectType.CLUSTER_SCHEMA, RepositoryObjectType.DATABASE);
    assertNotNull(sharedObjectsByType);
    assertEquals(2, sharedObjectsByType.size());

    @SuppressWarnings("unchecked")
    List<DatabaseMeta> databaseMetas = (List<DatabaseMeta>) sharedObjectsByType.get(RepositoryObjectType.DATABASE);
    assertNotNull(databaseMetas);
    assertEquals(1, databaseMetas.size());
    DatabaseMeta dbMetaResult = databaseMetas.get(0);
    assertNotNull(dbMetaResult);
    assertEquals(dbMeta, dbMetaResult);

    @SuppressWarnings("unchecked")
    List<ClusterSchema> clusterSchemas = (List<ClusterSchema>) sharedObjectsByType
        .get(RepositoryObjectType.CLUSTER_SCHEMA);
    assertNotNull(clusterSchemas);
    assertEquals(1, clusterSchemas.size());
    ClusterSchema clusterSchemaResult = clusterSchemas.get(0);
    assertNotNull(clusterSchemaResult);
    assertEquals(clusterSchema, clusterSchemaResult);

    repository.deleteDatabaseMeta(EXP_DBMETA_NAME);
    repository.deleteClusterSchema(clusterSchema.getObjectId());
  }

  private class MockProgressMonitorListener implements ProgressMonitorListener {

    @Override
    public void beginTask(String arg0, int arg1) {
    }

    @Override
    public void done() {
    }

    @Override
    public boolean isCanceled() {
      return false;
    }

    @Override
    public void setTaskName(String arg0) {
    }

    @Override
    public void subTask(String arg0) {
    }

    @Override
    public void worked(int arg0) {
    }
  }

  private class MockRepositoryExportParser extends DefaultHandler2 {
    private List<String> nodeNames = new ArrayList<String>();
    private SAXParseException fatalError;
    private List<String> nodesToCapture = Arrays.asList("repository", "transformations", "transformation", "jobs", "job"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    
    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
      // Only capture nodes we care about
      if (nodesToCapture.contains(qName)) {
        nodeNames.add(qName);
      }
    }
    
    @Override
    public void fatalError(SAXParseException e) throws SAXException {
      fatalError = e;
    }
    
    public List<String> getNodesWithName(String name) {
      List<String> nodes = new ArrayList<String>();
      for (String node : nodeNames) {
        if(node.equals(name)) {
          nodes.add(name);
        }
      }
      return nodes;
    }
    
    public List<String> getNodeNames() {
      return nodeNames;
    }
    
    public SAXParseException getFatalError() {
      return fatalError;
    }
  }
  
  @Test
  public void testExport() throws Exception {
    final String exportFileName = new File("test.export").getAbsolutePath(); //$NON-NLS-1$

    RepositoryDirectoryInterface rootDir = initRepo();
    String uniqueTransName = EXP_TRANS_NAME.concat(EXP_DBMETA_NAME);
    TransMeta transMeta = createTransMeta(EXP_DBMETA_NAME);

    // Create a database association
    DatabaseMeta dbMeta = createDatabaseMeta(EXP_DBMETA_NAME);
    repository.save(dbMeta, VERSION_COMMENT_V1, null);

    TableInputMeta tableInputMeta = new TableInputMeta();
    tableInputMeta.setDatabaseMeta(dbMeta);

    transMeta.addStep(new StepMeta(EXP_TRANS_STEP_1_NAME, tableInputMeta));

    RepositoryDirectoryInterface transDir = rootDir.findDirectory(DIR_TRANSFORMATIONS);
    repository.save(transMeta, VERSION_COMMENT_V1, null);
    deleteStack.push(transMeta); // So this transformation is cleaned up afterward
    assertNotNull(transMeta.getObjectId());
    ObjectRevision version = transMeta.getObjectRevision();
    assertNotNull(version);
    assertTrue(hasVersionWithComment(transMeta, VERSION_COMMENT_V1));
    assertTrue(repository.exists(uniqueTransName, transDir, RepositoryObjectType.TRANSFORMATION));

    JobMeta jobMeta = createJobMeta(EXP_JOB_NAME);
    RepositoryDirectoryInterface jobsDir = rootDir.findDirectory(DIR_JOBS);
    repository.save(jobMeta, VERSION_COMMENT_V1, null);
    deleteStack.push(jobMeta);
    assertNotNull(jobMeta.getObjectId());
    version = jobMeta.getObjectRevision();
    assertNotNull(version);
    assertTrue(hasVersionWithComment(jobMeta, VERSION_COMMENT_V1));
    assertTrue(repository.exists(EXP_JOB_NAME, jobsDir, RepositoryObjectType.JOB));

    try {
      repository.getExporter().exportAllObjects(new MockProgressMonitorListener(), exportFileName, null, "all"); //$NON-NLS-1$
      FileObject exportFile = KettleVFS.getFileObject(exportFileName);
      assertNotNull(exportFile);      
      MockRepositoryExportParser parser = new MockRepositoryExportParser();
      SAXParserFactory.newInstance().newSAXParser().parse(KettleVFS.getInputStream(exportFile), parser);
      if (parser.getFatalError() != null) {
        throw parser.getFatalError();
      }
      assertNotNull("No nodes found in export", parser.getNodeNames()); //$NON-NLS-1$
      assertTrue("No nodes found in export", !parser.getNodeNames().isEmpty()); //$NON-NLS-1$
      assertEquals("Incorrect number of nodes", 5, parser.getNodeNames().size()); //$NON-NLS-1$
      assertEquals("Incorrect number of transformations", 1, parser.getNodesWithName("transformation").size()); //$NON-NLS-1$ //$NON-NLS-2$
      assertEquals("Incorrect number of jobs", 1, parser.getNodesWithName("job").size()); //$NON-NLS-1$ //$NON-NLS-2$
    } finally {
      KettleVFS.getFileObject(exportFileName).delete();
    }
  }
}
