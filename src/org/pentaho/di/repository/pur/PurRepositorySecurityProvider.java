package org.pentaho.di.repository.pur;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.BaseRepositorySecurityProvider;
import org.pentaho.di.repository.IRole;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositorySecurityManager;
import org.pentaho.di.repository.RepositorySecurityProvider;
import org.pentaho.di.repository.RoleInfo;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.platform.engine.security.userroledao.ws.UserRoleException;

public class PurRepositorySecurityProvider extends BaseRepositorySecurityProvider implements RepositorySecurityProvider, RepositorySecurityManager, IUserRoleListChangeListener {

	private PurRepository	repository;
	private UserRoleDelegate	userRoleDelegate;
	private UserRoleListDelegate userRoleListDelegate;
	private static final Log logger = LogFactory.getLog(UserRoleDelegate.class);
	
  public PurRepositorySecurityProvider(PurRepository repository, PurRepositoryMeta repositoryMeta, UserInfo userInfo) {
		super(repositoryMeta, userInfo);
		this.repository = repository;
    this.userRoleListDelegate = new UserRoleListDelegate(repositoryMeta, userInfo, logger);
    this.userRoleDelegate = new UserRoleDelegate(this, repositoryMeta, userInfo, logger);
    userRoleDelegate.addUserRoleListChangeListener(this);
    this.setUserRoleDelegate(userRoleDelegate);
    this.setUserRoleListDelegate(userRoleListDelegate);
	}

	public UserRoleDelegate getUserRoleDelegate() {
		return userRoleDelegate;
	}

	public void setUserRoleDelegate(UserRoleDelegate userRoleDelegate) {
		this.userRoleDelegate = userRoleDelegate;
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

	public boolean supportsMetadata() {
		return true;
	}

	public boolean supportsRevisions() {
		return true;
	}

	public boolean supportsUsers() {
		return true;
	}

	public boolean allowsVersionComments() {
		return true;
	}

	public void delUser(ObjectId id_user) throws KettleException {
	}

	public ObjectId getUserID(String login) throws KettleException {
		return null;
	}

	public ObjectId[] getUserIDs() throws KettleException {
		return null;
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

	public UserInfo loadUserInfo(String login) throws KettleException {
    // Create a UserInfo object
    UserInfo user = new UserInfo(login);
    user.setName(login);
    return user;
	}

	public UserInfo loadUserInfo(String login, String password) throws KettleException {
    // Create a UserInfo object
      UserInfo user = new UserInfo(login);
      user.setPassword(password);
      user.setName(login);
      return user;
	}

	public void renameUser(ObjectId id_user, String newname) throws KettleException {
	}

	public void saveUserInfo(UserInfo userInfo) throws KettleException {
		userRoleDelegate.createUser(userInfo);
	}

	public void createRole(IRole newRole) throws KettleException {
		userRoleDelegate.createRole(newRole);
	}

	public void deleteRoles(List<IRole> roles) throws KettleException {
		userRoleDelegate.deleteRoles(roles);
	}

	 public void deleteUsers(List<UserInfo> users) throws KettleException {
	    userRoleDelegate.deleteUsers(users);
	  }

	public IRole getRole(String name) throws KettleException {
		return userRoleDelegate.getRole(name);
	}


	public List<IRole> getRoles() throws KettleException {
		return userRoleDelegate.getRoles();
	}

	public void updateRole(IRole role) throws KettleException {
		userRoleDelegate.updateRole(role);		
	}

	public void updateUser(UserInfo userInfo) throws KettleException {
		userRoleDelegate.updateUser(userInfo);
	}
	public void delUser(String name) throws KettleException {
		userRoleDelegate.deleteUser(name);
		
	}

	public void deleteRole(String name) throws KettleException {
		userRoleDelegate.deleteRole(name);
		
	}

	public List<UserInfo> getUsers() throws KettleException {
		return userRoleDelegate.getUsers();
	}

	public void setRoles(List<IRole> roles) throws KettleException {
		userRoleDelegate.setRoles(roles);
		
	}

	public void setUsers(List<UserInfo> users) throws KettleException {
		userRoleDelegate.setUsers(users);
	}

  public List<String> getAllRoles() throws KettleException {
    return userRoleListDelegate.getAllRoles();
  }

  public List<String> getAllUsers() throws KettleException {
    return userRoleListDelegate.getAllUsers();
  }

  public void setUserRoleListDelegate(UserRoleListDelegate userRoleListDelegate) {
    this.userRoleListDelegate = userRoleListDelegate;
  }

  public UserRoleListDelegate getUserRoleListDelegate() {
    return userRoleListDelegate;
  }

  public IRole constructRole() throws KettleException {
    return new RoleInfo();
  }

  public void onChange() {
    
    try {
      userRoleListDelegate.updateUserRoleList();
      userRoleDelegate.updateUserRoleInfo();
    } catch (UserRoleException e) {
      e.printStackTrace();
    }
  }
  public static Log getLogger() {
    return logger;
  }


}
