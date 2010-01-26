package org.pentaho.di.repository.jcr;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.RoleInfo;
import org.pentaho.di.repository.UserInfo;

public class JCRRepositoryUserRoleDelegate  extends JCRRepositoryBaseDelegate {

	//'Admin','Super User'
	//'Anonymous','User has not logged in'
	//Authenticated','User has logged in'
	//ceo','Chief Executive Officer'
	//'cto','Chief Technology Officer'
	//'dev','Developer'
	//'devmgr','Development Manager');
	//'is','Information Services');
	
	private List<UserInfo> users = new ArrayList<UserInfo>();
	private List<RoleInfo> roles = new ArrayList<RoleInfo>();

	public JCRRepositoryUserRoleDelegate(JCRRepository repository) {
		super(repository);
		roles.add(new RoleInfo("Admin","Super User"));
		roles.add(new RoleInfo("Anonymous","User has not logged in"));
		roles.add(new RoleInfo("Authenticated","User has logged in"));
		roles.add(new RoleInfo("ceo","Chief Executive Officer"));
		roles.add(new RoleInfo("cto","Chief Technology Officer"));
		roles.add(new RoleInfo("dev","Developer"));
		roles.add(new RoleInfo("devmgr","Development Manager"));
		roles.add(new RoleInfo("is","Information Services"));
		UserInfo joeUser = new UserInfo("joe", "password", "joe","joe", true);
		UserInfo patUser = new UserInfo("pat", "password", "pat","pat", true);
		UserInfo suzyUser = new UserInfo("suzy", "password", "suzy","suzy", true);
		UserInfo tiffanyUser = new UserInfo("tiffany", "password", "tiffany","tiffany", true);
		joeUser.addRole(roles.get(0));
		joeUser.addRole(roles.get(2));
		joeUser.addRole(roles.get(3));

		suzyUser.addRole(roles.get(2));
		suzyUser.addRole(roles.get(4));
		suzyUser.addRole(roles.get(7));

		patUser.addRole(roles.get(2));
		patUser.addRole(roles.get(5));

		tiffanyUser.addRole(roles.get(2));
		tiffanyUser.addRole(roles.get(5));
		tiffanyUser.addRole(roles.get(6));
		
		users.add(joeUser);
		users.add(patUser);
		users.add(suzyUser);
		users.add(tiffanyUser);
	}
	public void createUser(UserInfo newUser) throws KettleException {
		// TODO convert the newUser to what the new service accepts
		// TODO call the new service create user method userroleservice.createUser(converted user);
		// TODO catch any exception that the service will throw and convert them to KettleException and throw it
		users.add(newUser);
	}

	public void deleteUser(UserInfo user) throws KettleException {
		// TODO convert the user to what the new service accepts
		// TODO call the new service delete user method userroleservice.deleteUser(converted user);
		// TODO catch any exception that the service will throw and convert them to KettleException and throw it
		users.remove(user);

	}

	public void deleteUser(String name) throws KettleException {
		// TODO convert the user to what the new service accepts
		// TODO call the new service delete user method userroleservice.deleteUser(converted user);
		// TODO catch any exception that the service will throw and convert them to KettleException and throw it
		for(UserInfo user:users) {
			if(user.getLogin().equals(name)) {
				deleteUser(user);
				break;
			}
		}
		
	}

	public void setUsers(List<UserInfo> users) throws KettleException {
		// TODO convert the user list to what the new service accepts
		// TODO call the new service delete user method userroleservice.setUsers(list of converted users);
		// TODO catch any exception that the service will throw and convert them to KettleException and throw it
		users.clear();
		users.addAll(users);
		
	}

	public UserInfo getUser(String name, String password) throws KettleException {
		// TODO call the new service get user method userroleservice.getUser(name, password);
		// TODO Convert what will be returned by the service to the User object and return
		// TODO delete the line below
		for(UserInfo user:users) {
			if(user.getLogin().equals(name) && user.getPassword().equals(password)) {
				return user;
			}
		}
		return null;
	}

	public UserInfo getUser(String name) throws KettleException {
		// TODO call the new service get user method userroleservice.getUser(name);
		// TODO Convert what will be returned by the service to the User object and return
		// TODO delete the line below
		for(UserInfo user:users) {
			if(user.getLogin().equals(name)) {
				return user;
			}
		}
		return null;
	}

	public List<UserInfo> getUsers() throws KettleException {
		// TODO call the new service get user method userroleservice.getUsers();
		// TODO Iterate over the list and convert what will be returned by the service to the User object and return
		// TODO delete the line below
		return users;
	}

	public void updateUser(UserInfo user) throws KettleException {
		// TODO convert the user to what the new service accepts
		// TODO call the new service update user method userroleservice.updateUser(converted user);
		// TODO catch any exception that the service will throw and convert them to KettleException and throw it
		UserInfo userInfo = null;
		for(UserInfo userinfo:users) {
			if(userinfo.getLogin().equals(user.getName())) {
				userInfo = userinfo;
				break;
			}
		}	
		userInfo.setDescription(user.getDescription());
		userInfo.setPassword(user.getPassword());
		userInfo.setRoles(user.getRoles());;
	}

	public void createRole(RoleInfo newRole) throws KettleException {
		// TODO convert the newRole to what the new service accepts
		// TODO call the new service create role method userroleservice.createRole(converted role);
		// TODO catch any exception that the service will throw and convert them to KettleException and throw it
		roles.add(newRole);
	}
	

	public void deleteRole(RoleInfo role) throws KettleException {
		// TODO convert the role to what the new service accepts
		// TODO call the new service delete role method userroleservice.deleteRole(converted role);
		// TODO catch any exception that the service will throw and convert them to KettleException and throw it
		roles.remove(role);
	}

	public RoleInfo getRole(String name) throws KettleException {
		// TODO call the new service get role method userroleservice.getRole(name);
		// TODO Convert what will be returned by the service to the Role object and return
		// TODO delete the line below
		for(RoleInfo role:roles) {
			if(role.getName().equals(name)) {
				return role;
			}
		}
		return null;
	}

	public List<RoleInfo> getRoles() throws KettleException {
		// TODO call the new service get roles method userroleservice.getRoles();
		// TODO Iterate over the list and convert what will be returned by the service to the Role object and return
		// TODO delete the line below
		return roles;
	}

	public void updateRole(RoleInfo role) throws KettleException {
		// TODO convert the role to what the new service accepts
		// TODO call the new service update role method userroleservice.updateRole(converted role);
		// TODO catch any exception that the service will throw and convert them to KettleException and throw it
		
		RoleInfo roleInfo = null;
		for(RoleInfo roleinfo:roles) {
			if(roleinfo.getName().equals(role.getName())) {
				roleInfo = roleinfo;
				break;
			}
		}	
		roleInfo.setDescription(role.getDescription());
		roleInfo.setUsers(role.getUsers());
	}
	
	public void deleteRole(String name) throws KettleException {
		// TODO call the new service get roles method userroleservice.deleteRole(name);
		for(RoleInfo role:roles) {
			if(role.getName().equals(name)) {
				deleteRole(role);
				break;
			}
		}
	}

	public void setRoles(List<RoleInfo> roles) throws KettleException {
		// TODO convert the role list to what the new service accepts
		// TODO call the new service update role method userroleservice.setRoles(converted role list);
		// TODO catch any exception that the service will throw and convert them to KettleException and throw it
		this.roles.clear();
		this.roles.addAll(roles);
	}

}
