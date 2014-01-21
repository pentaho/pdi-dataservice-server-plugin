/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package org.pentaho.di.repository;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.job.JobMeta;
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
public class RepositoryPerformanceApp extends RepositoryTestBase implements java.io.Serializable {
  static final long serialVersionUID = -5389269822527972858L; /* EESOURCE: UPDATE SERIALVERUID */

  
  public RepositoryPerformanceApp(String url) {
    super();
    setRepositoryLocation(url);
  }

  private static int lightLoadMax = 5;
  private static int moderateLoadMax = 50;
  private static int heavyLoadMax = 500;
  private static int contentLoadMax = 5;

  private static String testFolder = "test_directory";
  
  private String repositoryLocation = null;

  public static void main(String[] args){
    
    String url = "http://localhost:9080/pentaho-di";
    if (args.length>0){
      url = args[0];
    }
    RepositoryPerformanceApp test = new RepositoryPerformanceApp(url);
    try {
      test.setUp();
      test.startupRepository();
      test.testLightLoad();
    } catch (Exception e) {
      e.printStackTrace();
    }
    
  }
  
  protected void startupRepository() throws Exception{
    // PentahoLicenseVerifier.setStreamOpener(new TestLicenseStream("pdi-ee=true")); //$NON-NLS-1$
    KettleEnvironment.init();

    repositoryMeta = new PurRepositoryMeta();
    repositoryMeta.setName("JackRabbit");
    repositoryMeta.setDescription("JackRabbit test repository");
    ((PurRepositoryMeta) repositoryMeta).setRepositoryLocation(new PurRepositoryLocation(
        repositoryLocation));
    userInfo = new UserInfo(EXP_LOGIN, "password", EXP_USERNAME, "Apache Tomcat user", true);
    repository = new PurRepository();
    
    repository.init(repositoryMeta);
    repository.connect(EXP_LOGIN, "password");
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
    RepositoryDirectoryInterface rootDir = loadStartDirectory();

    for (int i = 0; i < max; i++) {
      
      RepositoryDirectoryInterface childDir = repository.createRepositoryDirectory(rootDir,  
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

  private void createContent(int loadMax, RepositoryDirectoryInterface createHere) throws Exception{
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
    
    private void createDirectories(int loadMax, RepositoryDirectoryInterface createHere) throws Exception{
      for (int ix = 0; ix < loadMax; ix++) {
        repository.createRepositoryDirectory(createHere, testFolder.concat(String.valueOf(ix)));
      }
    }

    public void setRepositoryLocation(String repositoryLocation) {
      this.repositoryLocation = repositoryLocation;
    }

    @Override
    protected RepositoryDirectoryInterface loadStartDirectory() throws Exception {
      RepositoryDirectoryInterface dir = super.loadStartDirectory();
      return dir.findDirectory("home/joe");
    }

    @Override
    protected void delete(ObjectId id) {
      // nothing to do
    }

    @Override
    public void setUp() throws Exception {
      super.setUp();  
      PentahoLicenseVerifier.setStreamOpener(new TestLicenseStream("biserver-ee=true\npdi-ee=true")); //$NON-NLS-1$
    }
    

}

