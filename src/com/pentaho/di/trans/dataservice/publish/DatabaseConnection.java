package com.pentaho.di.trans.dataservice.publish;

import java.util.HashMap;
import java.util.Map;

public class DatabaseConnection {

  public static final String CUSTOM_DRIVER_CLASS = "CUSTOM_DRIVER_CLASS";
  public static final String CUSTOM_URL = "CUSTOM_URL";

  private String accessType;
  private Map<String, String> attributes = new HashMap<String, String>();
  private boolean changed;
  private String username;
  private String password;
  private String name;
  private DatabaseType databaseType;

  public String getAccessType() {
    return accessType;
  }

  public void setAccessType( String accessType ) {
    this.accessType = accessType;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public void setAttributes( Map<String, String> attributes ) {
    this.attributes = attributes;
  }

  public void addAttribute( String key, String value ) {
    this.attributes.put( key, value );
  }

  public boolean isChanged() {
    return changed;
  }

  public void setChanged( boolean changed ) {
    this.changed = changed;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername( String username ) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword( String password ) {
    this.password = password;
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public DatabaseType getDatabaseType() {
    return databaseType;
  }

  public void setDatabaseType( DatabaseType databaseType ) {
    this.databaseType = databaseType;
  }
}
