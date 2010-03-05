package org.pentaho.di.repository;

import java.util.HashSet;
import java.util.Set;

public class EEUserInfo extends UserInfo implements IEEUser {

  private Set<IRole> roles;
  

  public EEUserInfo() {
    super();
    this.roles = new HashSet<IRole>();
  }

  public EEUserInfo(String login, String password, String username, String description, boolean enabled,
      Set<IRole> roles) {
    super(login, password, username, description, enabled);
    this.roles = roles;
  }

  public EEUserInfo(String login, String password, String username, String description, boolean enabled) {
    super(login, password, username, description, enabled);
    this.roles = new HashSet<IRole>();
  }

  public EEUserInfo(String login) {
    super(login);
    this.roles = new HashSet<IRole>();
  }

  public EEUserInfo(EEUserInfo copyFrom) {
    super(copyFrom);
    this.roles = copyFrom.roles != null ? new HashSet<IRole>(copyFrom.roles) : null;
  }

  public boolean addRole(IRole role) {
    return this.roles.add(role);
  }

  public boolean removeRole(IRole role) {
    return this.roles.remove(role);
  }

  public void clearRoles() {
    this.roles.clear();
  }

  public void setRoles(Set<IRole> roles) {
    this.roles = roles;
  }

  public Set<IRole> getRoles() {
    return this.roles;
  }

}
