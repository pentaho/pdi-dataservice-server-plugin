package org.pentaho.di.repository.pur;

import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.BaseRepositorySecurityProvider;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.ProfileMeta;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.RepositorySecurityProvider;
import org.pentaho.di.repository.RoleInfo;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.di.repository.ProfileMeta.Permission;

public class PurRepositorySecurityProvider extends BaseRepositorySecurityProvider implements RepositorySecurityProvider {

	private PurRepository	repository;
	private UserRoleDelegate	userRoleDelegate;
	private UserRoleListDelegate userRoleListDelegate;
	private PurRepositoryMeta repositoryMeta;

	public PurRepositorySecurityProvider(PurRepository repository, PurRepositoryMeta repositoryMeta, UserInfo userInfo) {
		super(repositoryMeta, userInfo);
		this.repository = repository;		
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
	  return repositoryMeta.isVersionCommentMandatory();
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


	
	
	
	
	
	
	public void delProfile(ObjectId id_profile) throws KettleException {
	}

	public void delUser(ObjectId id_user) throws KettleException {
	}

	public ObjectId[] getPermissionIDs(ObjectId id_profile) throws KettleException {
		return null;
	}

	public ObjectId getProfileID(String profilename) throws KettleException {
		return null;
	}

	public String[] getProfiles() throws KettleException {
		return null;
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

	public Permission loadPermission(ObjectId id_permission) throws KettleException {
		return null;
	}

	public ProfileMeta loadProfileMeta(ObjectId id_profile) throws KettleException {
		return null;
	}

	public UserInfo loadUserInfo(String login) throws KettleException {
		return userRoleDelegate.getUser(login);
	}

	public UserInfo loadUserInfo(String login, String password) throws KettleException {
		return userRoleDelegate.getUser(login);
	}

	public void renameProfile(ObjectId id_profile, String newname) throws KettleException {
	}

	public void renameUser(ObjectId id_user, String newname) throws KettleException {
	}

	public void saveProfile(ProfileMeta profileMeta) throws KettleException {
	}

	public void saveUserInfo(UserInfo userInfo) throws KettleException {
		userRoleDelegate.createUser(userInfo);
	}

	public void createRole(RoleInfo newRole) throws KettleException {
		userRoleDelegate.createRole(newRole);
	}

	public void deleteRoles(List<RoleInfo> roles) throws KettleException {
		userRoleDelegate.deleteRoles(roles);
	}

	 public void deleteUsers(List<UserInfo> users) throws KettleException {
	    userRoleDelegate.deleteUsers(users);
	  }

	public RoleInfo getRole(String name) throws KettleException {
		return userRoleDelegate.getRole(name);
	}


	public List<RoleInfo> getRoles() throws KettleException {
		return userRoleDelegate.getRoles();
	}

	public void updateRole(RoleInfo role) throws KettleException {
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

	public void setRoles(List<RoleInfo> roles) throws KettleException {
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

  public List<String> getAllUsersInRole(String role) throws KettleException {
    return userRoleListDelegate.getAllUsersInRole(role);
  }

  public List<String> getRolesForUser(String userName) throws KettleException {
    return userRoleListDelegate.getRolesForUser(userName);
  }

  public void setUserRoleListDelegate(UserRoleListDelegate userRoleListDelegate) {
    this.userRoleListDelegate = userRoleListDelegate;
  }

  public UserRoleListDelegate getUserRoleListDelegate() {
    return userRoleListDelegate;
  }
	
}
