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

/**
 * This test class is not meant to be run automated. It provides two simple purposes:
 * 1. to bulk load a PUR repository
 * 2. to give simple stats on the performance of the load.
 * 
 * Simple pass a repository URL either on the command line as an argument, or set it
 * in the code, and run the main method. The repository should be empty on initial
 * execution, and you cannot run this utility against the same repo twice without first emptying the repo, 
 * as the execution will throw duplicate entry exceptions. 
 * 
 * The class can be improved by passing a load parameter as well. 
 * 
 * @author GMoran
 *
 */
public class RepositoryPerformanceApp extends RepositoryTestBase {
  
  public RepositoryPerformanceApp(String url) {
    super();
    setRepositoryLocation(url);
  }

  private static int lightLoadMax = 1;
  private static int moderateLoadMax = 50;
  private static int heavyLoadMax = 500;
  private static int contentLoadMax = 5;

  private static String testFolder = "test_directory";
  
  private String repositoryLocation = null;

  public static void main(String[] args){
    
    String url = "http://localhost:8080/pentaho/webservices";
    if (args.length>0){
      url = args[0];
    }
    RepositoryPerformanceApp test = new RepositoryPerformanceApp(url);
    try {
      test.setUp();
      test.startupRepository();
      test.testModerateLoad();
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

  private Long testLoad(int max) throws Exception{
    
    Long timeInMillis = System.currentTimeMillis();
    RepositoryDirectory rootDir = loadStartDirectory();

    for (int i = 0; i < max; i++) {
      
      RepositoryDirectory childDir = repository.createRepositoryDirectory(rootDir,  
                                                      testFolder.concat(String.valueOf(i)));
      createContent(contentLoadMax, childDir);
      createDirectories(contentLoadMax, childDir);
    }
    Long endTimeInMillis = System.currentTimeMillis();
    Long exec = endTimeInMillis - timeInMillis;
    
    System.out.println("Execution time in seconds: ".concat(String.valueOf(exec*0.001)).concat("s"));
    System.out.println("Created ".concat(String.valueOf(max * ((contentLoadMax*2) + 1))).concat(" primary PDI objects. "));

    return exec;
  }

  private void createContent(int loadMax, RepositoryDirectory createHere) throws Exception{
      for (int ix = 0; ix < loadMax; ix++) {
        TransMeta transMeta = createTransMeta(createHere.getName().concat(EXP_DBMETA_NAME.concat(String.valueOf(ix))));
        transMeta.setRepositoryDirectory(createHere);
        try{
          repository.save(transMeta, VERSION_COMMENT_V1.concat(String.valueOf(ix)), null);
        }catch(Exception e){
        }

        JobMeta jobMeta = createJobMeta("JOB_".concat(createHere.getName()).concat(EXP_DBMETA_NAME.concat(String.valueOf(ix))));
        jobMeta.setRepositoryDirectory(createHere);
        try{
          repository.save(jobMeta, VERSION_COMMENT_V1.concat(String.valueOf(ix)), null);
        }catch(Exception e){
        }
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
      return dir.findDirectory("home/joe");
    }

    @Override
    protected void delete(ObjectId id) {
      // nothing to do
    }
    

}

