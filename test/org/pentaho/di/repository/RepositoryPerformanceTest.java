package org.pentaho.di.repository;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.ProfileMeta.Permission;
import org.pentaho.di.repository.pur.PurRepository;
import org.pentaho.di.repository.pur.PurRepositoryLocation;
import org.pentaho.di.repository.pur.PurRepositoryMeta;
import org.pentaho.di.trans.TransMeta;

import com.pentaho.commons.dsc.PentahoLicenseVerifier;
import com.pentaho.commons.dsc.util.TestLicenseStream;

public class RepositoryPerformanceTest extends RepositoryTestBase {
  
  public RepositoryPerformanceTest(String url) {
    super();
    setRepositoryLocation(url);
  }

  private static int lightLoadMax = 1;
  private static int moderateLoadMax = 50;
  private static int heavyLoadMax = 500;
  private static int contentLoadMax = 5;

  private static String testFolder = "test_directory";
  private static String testTrans = "test_transformation";
  private static String testJob = "test_job";
  
  private String repositoryLocation = null;

  public static void main(String[] args){
    RepositoryPerformanceTest test = 
          new RepositoryPerformanceTest("http://localhost:8080/pentaho/webservices/repo");
    try {
      test.startupRepository();
      test.testLightLoad();
    } catch (Exception e) {
      e.printStackTrace();
    }
    
  }
  
  protected void startupRepository() throws Exception{
    System.setProperty(Const.KETTLE_PLUGIN_PACKAGES, RepositoryTestBase.class.getPackage().getName());
    PentahoLicenseVerifier.setStreamOpener(new TestLicenseStream("pdi-ee=true")); //$NON-NLS-1$
    KettleEnvironment.init();

    repositoryMeta = new PurRepositoryMeta();
    repositoryMeta.setName("JackRabbit");
    repositoryMeta.setDescription("JackRabbit test repository");
    ((PurRepositoryMeta) repositoryMeta).setRepositoryLocation(new PurRepositoryLocation(
        repositoryLocation));
    ProfileMeta adminProfile = new ProfileMeta("admin", "Administrator");
    adminProfile.addPermission(Permission.ADMIN);
    userInfo = new UserInfo(EXP_LOGIN, "password", EXP_USERNAME, "Apache Tomcat user", true, adminProfile);
    repository = new PurRepository();
    
    repository.init(repositoryMeta, userInfo);
    repository.connect();
  }
  
  public void testLightLoad() throws Exception{
    testLoad(lightLoadMax);
  }

  public void testModerateLoad() throws Exception{
    testLoad(moderateLoadMax);
  }

  public void testHeavyLoad() throws Exception{
    testLoad(heavyLoadMax);
  }

  private void testLoad(int max) throws Exception{
    
    RepositoryDirectory rootDir = loadStartDirectory();

    for (int i = 0; i < max; i++) {
      
      RepositoryDirectory childDir = repository.createRepositoryDirectory(rootDir,  
                                                      testFolder.concat(String.valueOf(i)));
      createContent(contentLoadMax, childDir);
      createDirectories(contentLoadMax, childDir);
    }
  }

  private void createContent(int loadMax, RepositoryDirectory createHere) throws Exception{
      for (int ix = 0; ix < loadMax; ix++) {
        TransMeta transMeta = createTransMeta(createHere.getName().concat(EXP_DBMETA_NAME.concat(String.valueOf(ix))));
        transMeta.setRepositoryDirectory(createHere);
        repository.save(transMeta, VERSION_COMMENT_V1.concat(String.valueOf(ix)), null);

        JobMeta jobMeta = createJobMeta("JOB_".concat(createHere.getName()).concat(EXP_DBMETA_NAME.concat(String.valueOf(ix))));
        jobMeta.setRepositoryDirectory(createHere);
        repository.save(jobMeta, VERSION_COMMENT_V1.concat(String.valueOf(ix)), null);
      }
    }
    
    private void createDirectories(int loadMax, RepositoryDirectory createHere) throws Exception{
      for (int ix = 0; ix < loadMax; ix++) {
        repository.createRepositoryDirectory(createHere, testFolder.concat(String.valueOf(ix)));
      }
    }

    public void setRepositoryLocation(String repositoryLocation) {
      this.repositoryLocation = repositoryLocation;
    }

    @Override
    protected RepositoryDirectory loadStartDirectory() throws Exception {
      RepositoryDirectory dir = super.loadStartDirectory();
      return dir.findDirectory("pentaho/tenant0/home/joe");
    }
    

}

