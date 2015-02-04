package com.pentaho.di.trans.dataservice.publish;

/**
 * Created by bmorrise on 12/9/14.
 */
public class DatabaseType {

  public static final String GENERIC = "GENERIC";

  public int defaultDatabasePort = -1;
  public String extraOptionsHelpUrl;
  public String name;
  public String shortName;

  public int getDefaultDatabasePort() {
    return defaultDatabasePort;
  }

  public void setDefaultDatabasePort( int defaultDatabasePort ) {
    this.defaultDatabasePort = defaultDatabasePort;
  }

  public String getExtraOptionsHelpUrl() {
    return extraOptionsHelpUrl;
  }

  public void setExtraOptionsHelpUrl( String extraOptionsHelpUrl ) {
    this.extraOptionsHelpUrl = extraOptionsHelpUrl;
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getShortName() {
    return shortName;
  }

  public void setShortName( String shortName ) {
    this.shortName = shortName;
  }
}
