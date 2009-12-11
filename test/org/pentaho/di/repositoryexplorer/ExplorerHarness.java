package org.pentaho.di.repositoryexplorer;

import org.eclipse.swt.widgets.MessageBox;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleSecurityException;
import org.pentaho.di.repository.Directory;
import org.pentaho.di.repository.ProfileMeta;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.RepositoryElementLocationInterface;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.di.repository.ProfileMeta.Permission;
import org.pentaho.di.repository.jcr.JCRRepository;
import org.pentaho.di.repository.jcr.JCRRepositoryLocation;
import org.pentaho.di.repository.jcr.JCRRepositoryMeta;
import org.pentaho.di.ui.repository.repositoryexplorer.RepositoryExplorer;
import org.pentaho.di.ui.repository.repositoryexplorer.RepositoryExplorerCallback;

public class ExplorerHarness {

  private JCRRepositoryMeta repositoryMeta;
  private JCRRepository repository;
  private UserInfo userInfo;

  private RepositoryDirectory directoryTree;

  /**
   * @param args
   */
  public static void main(String[] args) {
    
    JCRRepositoryMeta repositoryMeta;
    JCRRepository repository;
    UserInfo userInfo;

    repositoryMeta = new JCRRepositoryMeta();
    repositoryMeta.setName("JackRabbit");
    repositoryMeta.setDescription("JackRabbit test repository");
    repositoryMeta.setRepositoryLocation(new JCRRepositoryLocation("http://localhost:8080/jackrabbit/rmi"));
    
    ProfileMeta adminProfile = new ProfileMeta("admin", "Administrator");
    adminProfile.addPermission(Permission.ADMIN);
    
    userInfo = new UserInfo("joe", "password", "Apache Tomcat", "Apache Tomcat user", true, adminProfile);
    
    repository = new JCRRepository();
    repository.init(repositoryMeta, userInfo);
    
    RepositoryExplorerCallback cb = new RepositoryExplorerCallback() {

        public boolean open(RepositoryElementLocationInterface element, String revision) {
          System.out.println("Name: ".concat(element.getName()));
          System.out.println("Type: ".concat(element.getRepositoryElementType().name()));
          System.out.println("Directory: ".concat(element.getRepositoryDirectory().toString()));
          System.out.println("Revision: ".concat(revision==null?"null":revision));
          return false; // do not close explorer
        }
    };

    
    try {
      repository.connect();
      Directory root = repository.loadRepositoryDirectoryTree();
      RepositoryExplorer explorer = new RepositoryExplorer(root, repository, cb, null);
      explorer.show();
    } catch (KettleSecurityException e) {
      e.printStackTrace();
    } catch (KettleException e) {
      e.printStackTrace();
    }
    

  }

}
