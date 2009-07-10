package org.pentaho.di.repository.jcr;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.BaseRepositorySecurityProvider;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.ProfileMeta;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.RepositorySecurityProvider;
import org.pentaho.di.repository.UserInfo;
import org.pentaho.di.repository.ProfileMeta.Permission;

public class JCRRepositorySecurityProvider extends BaseRepositorySecurityProvider implements RepositorySecurityProvider {

	private JCRRepository	repository;

	public JCRRepositorySecurityProvider(JCRRepository repository, RepositoryMeta repositoryMeta, UserInfo userInfo) {
		super(repositoryMeta, userInfo);
		this.repository = repository;
	}
	
	public JCRRepository getRepository() {
		return repository;
	}
	
	public boolean isVersionCommentMandatory() {
		return repository.getJcrRepositoryMeta().isVersionCommentMandatory();
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
		return null;
	}

	public Permission loadPermission(ObjectId id_permission) throws KettleException {
		return null;
	}

	public ProfileMeta loadProfileMeta(ObjectId id_profile) throws KettleException {
		return null;
	}

	public UserInfo loadUserInfo(String login) throws KettleException {
		return null;
	}

	public UserInfo loadUserInfo(String login, String password) throws KettleException {
		return null;
	}

	public void renameProfile(ObjectId id_profile, String newname) throws KettleException {
	}

	public void renameUser(ObjectId id_user, String newname) throws KettleException {
	}

	public void saveProfile(ProfileMeta profileMeta) throws KettleException {
	}

	public void saveUserInfo(UserInfo userInfo) throws KettleException {
	}	
}
