package org.pentaho.di.repository.pur;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.BaseRepositorySecurityProvider;
import org.pentaho.di.repository.IUser;
import org.pentaho.di.repository.RepositorySecurityProvider;

public class PurRepositorySecurityProvider extends BaseRepositorySecurityProvider implements RepositorySecurityProvider , IUserRoleListChangeListener{

	private PurRepository	repository;
  private UserRoleListDelegate userRoleListDelegate;
  private UserRoleDelegate  userRoleDelegate;
	private static final Log logger = LogFactory.getLog(PurRepositorySecurityProvider.class);
	
  public PurRepositorySecurityProvider(PurRepository repository, PurRepositoryMeta repositoryMeta, IUser user) {
		super(repositoryMeta, user);
		this.repository = repository;
    this.userRoleListDelegate = new UserRoleListDelegate(repositoryMeta, user, logger);
    this.setUserRoleListDelegate(userRoleListDelegate);
	}

	public PurRepository getRepository() {
		return repository;
	}
	
	public boolean isVersionCommentMandatory() {
	  return  ((PurRepositoryMeta) repositoryMeta).isVersionCommentMandatory();
	}

	public boolean isLockingPossible() {
		return true;
	}

	public boolean isReadOnly() {
		return false;
	}

	public boolean allowsVersionComments() {
		return true;
	}

  public String[] getUserLogins() throws KettleException {
    List<String> users = userRoleListDelegate.getAllUsers();
    if(users != null && users.size() > 0) {
      String[] returnValue = new String[users.size()];
      users.toArray(returnValue);
      return returnValue;
    }
    return null;
  }


  public List<String> getAllRoles() throws KettleException {
    return userRoleListDelegate.getAllRoles();
  }

  public List<String> getAllUsers() throws KettleException {
    return userRoleListDelegate.getAllUsers();
  }

  public UserRoleDelegate getUserRoleDelegate() {
    return userRoleDelegate;
  }

  public void setUserRoleDelegate(UserRoleDelegate userRoleDelegate) {
    this.userRoleDelegate = userRoleDelegate;
    this.userRoleDelegate.addUserRoleListChangeListener(this);
  }

  public void setUserRoleListDelegate(UserRoleListDelegate userRoleListDelegate) {
    this.userRoleListDelegate = userRoleListDelegate;
  }

  public UserRoleListDelegate getUserRoleListDelegate() {
    return userRoleListDelegate;
  }

  public void onChange() {
    userRoleListDelegate.updateUserRoleList();
  }

  public static Log getLogger() {
    return logger;
  }


}
