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

package com.pentaho.di.trans.dataservice.publish;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.FormDataMultiPart;
import com.sun.jersey.multipart.impl.MultiPartWriter;
import org.dom4j.DocumentHelper;
import org.pentaho.agilebi.modeler.ModelerException;
import org.pentaho.agilebi.modeler.ModelerPerspective;
import org.pentaho.agilebi.modeler.ModelerWorkspace;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.metadata.model.LogicalModel;
import org.pentaho.metadata.model.concept.types.LocalizedString;
import org.pentaho.metadata.util.MondrianModelExporter;

import javax.ws.rs.core.MediaType;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class ModelServerPublish {

  private static Class<?> PKG = ModelServerPublish.class;

  public static final int PUBLISH_FAILED = 2;
  public static final int PUBLISH_SUCCESS = 3;
  public static final int PUBLISH_INVALID_SERVER = 4;
  public static final int PUBLISH_VALID_SERVER = 5;
  public static final int PUBLISH_CATALOG_EXISTS = 8;
  private static final String MONDRIAN_POST_ANALYSIS_URL = "plugin/data-access/api/mondrian/postAnalysis";
  private static final String META_DATA_IMPORT_URL = "plugin/data-access/api/metadata/import";
  private static final String CONNECTIONS_URL = "plugin/data-access/api/datasource/jdbc/connection";
  private static final String PLUGIN_DATA_ACCESS_API_CONNECTION_UPDATE = "plugin/data-access/api/datasource/jdbc/connection";
  private static final String EXTENSION_XMI = ".xmi";

  private static final String DATABASE_DRIVER = "org.pentaho.di.core.jdbc.ThinDriver";
  private static final String DATABASE_CONNECTION = "jdbc:pdi://%s:%s/kettle?webappname=pentaho-di";
  private static final String ACCESS_TYPE = "NATIVE";

  private ModelerWorkspace model;

  private Client client = null;

  private BaServerConnection baServerConnection;

  public ModelServerPublish( BaServerConnection baServerConnection ) {
    this( null, baServerConnection );
  }

  public ModelServerPublish( ModelerWorkspace modelerWorkspace, BaServerConnection baServerConnection ) {

    this.model = modelerWorkspace;
    this.baServerConnection = baServerConnection;

    ClientConfig clientConfig = new DefaultClientConfig();
    clientConfig.getFeatures().put( JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE );
    clientConfig.getClasses().add( MultiPartWriter.class );
    this.client = Client.create( clientConfig );
    this.client
      .addFilter( new HTTPBasicAuthFilter( baServerConnection.getUsername(), baServerConnection.getPassword() ) );
  }

  public int testConnection() {

    WebResource webResource = client.resource( baServerConnection.getUrl() + CONNECTIONS_URL );

    int response;

    try {
      ClientResponse resp = webResource.get( ClientResponse.class );
      if ( resp != null && resp.getStatus() == 200 ) {
        response = ModelServerPublish.PUBLISH_VALID_SERVER;
      } else {
        response = ModelServerPublish.PUBLISH_INVALID_SERVER;
      }
    } catch ( Exception e ) {
      response = ModelServerPublish.PUBLISH_INVALID_SERVER;
    }

    return response;
  }

  public boolean publishDataSource( DatabaseMeta databaseMeta ) throws KettleDatabaseException {

    String host = databaseMeta.getHostname();
    String port = databaseMeta.getDatabasePortNumberString();

    String connectionUrl = String.format( DATABASE_CONNECTION, host, port );

    DatabaseConnection connection = new DatabaseConnection();
    connection.setAccessType( ACCESS_TYPE );
    connection.setName( databaseMeta.getName() );
    connection.setPassword( databaseMeta.getPassword() );
    connection.setUsername( databaseMeta.getUsername() );
    connection.addAttribute( DatabaseConnection.CUSTOM_DRIVER_CLASS, DATABASE_DRIVER );
    connection.addAttribute( DatabaseConnection.CUSTOM_URL, connectionUrl );

    DatabaseType databaseType = new DatabaseType();
    databaseType.setShortName( DatabaseType.GENERIC );
    connection.setDatabaseType( databaseType );

    return updateConnection( connection );
  }

  private boolean updateConnection( DatabaseConnection connection ) {
    String storeDomainUrl;
    try {
      storeDomainUrl = baServerConnection.getUrl() + PLUGIN_DATA_ACCESS_API_CONNECTION_UPDATE + "/" + connection.getName();
      WebResource resource = client.resource( storeDomainUrl );
      WebResource.Builder builder = resource
        .type( MediaType.APPLICATION_JSON )
        .entity( connection );
      ClientResponse resp = builder.put( ClientResponse.class );
      if ( resp != null && resp.getStatus() != 200 ) {
        return false;
      }
    } catch ( Exception ex ) {
      return false;
    }
    return true;
  }

  public int publishMondrainSchema( InputStream mondrianFile, String catalogName, String datasourceInfo,
                                    boolean overwriteInRepos ) throws Exception {
    String storeDomainUrl = baServerConnection.getUrl() + MONDRIAN_POST_ANALYSIS_URL;
    WebResource resource = client.resource( storeDomainUrl );
    String params = "Datasource=" + datasourceInfo;
    int response = ModelServerPublish.PUBLISH_FAILED;
    FormDataMultiPart part = new FormDataMultiPart();
    part.field( "parameters", params, MediaType.MULTIPART_FORM_DATA_TYPE )
      .field( "uploadAnalysis", mondrianFile, MediaType.MULTIPART_FORM_DATA_TYPE )
      .field( "catalogName", catalogName, MediaType.MULTIPART_FORM_DATA_TYPE )
      .field( "overwrite", overwriteInRepos ? "true" : "false", MediaType.MULTIPART_FORM_DATA_TYPE )
      .field( "xmlaEnabledFlag", "true", MediaType.MULTIPART_FORM_DATA_TYPE );

    part.getField( "uploadAnalysis" ).setContentDisposition(
      FormDataContentDisposition.name( "uploadAnalysis" ).fileName( catalogName ).build() );
    try {
      ClientResponse resp = resource
        .type( MediaType.MULTIPART_FORM_DATA_TYPE )
        .post( ClientResponse.class, part );
      String entity = null;
      if ( resp != null && resp.getStatus() == 200 ) {
        entity = resp.getEntity( String.class );
        if ( entity.equals( String.valueOf( ModelServerPublish.PUBLISH_CATALOG_EXISTS ) ) ) {
          response = ModelServerPublish.PUBLISH_CATALOG_EXISTS;
        } else {
          response = Integer.parseInt( entity );
        }
      }
    } catch ( Exception ex ) {
      // Do Nothing
    }
    return response;
  }

  public int publishMetaDataFile( InputStream metadataFile, String domainId ) throws Exception {
    String storeDomainUrl = baServerConnection.getUrl() + META_DATA_IMPORT_URL;
    WebResource resource = client.resource( storeDomainUrl );

    int response = ModelServerPublish.PUBLISH_FAILED;
    FormDataMultiPart part = new FormDataMultiPart();
    part.field( "domainId", domainId, MediaType.MULTIPART_FORM_DATA_TYPE )
      .field( "metadataFile", metadataFile, MediaType.MULTIPART_FORM_DATA_TYPE );
    part.getField( "metadataFile" ).setContentDisposition(
      FormDataContentDisposition.name( "metadataFile" )
        .fileName( domainId ).build() );
    try {
      ClientResponse resp = resource
        .type( MediaType.MULTIPART_FORM_DATA_TYPE )
        .put( ClientResponse.class, part );
      if ( resp != null && ( resp.getStatus() == 200 || resp.getStatus() == 3 ) ) {
        response = ModelServerPublish.PUBLISH_SUCCESS;
      }
    } catch ( Exception ex ) {
      // Do Nothing
    }
    return response;
  }

  public int publishToServer( String schemaName,
                              String jndiName, String modelName,
                              String publishModelFileName ) throws Exception {

    int result = publishOlapSchemaToServer( schemaName, jndiName, modelName, true );

    if ( result == ModelServerPublish.PUBLISH_SUCCESS ) {
      result = publishMetaDatafile( publishModelFileName, modelName + EXTENSION_XMI );
    }

    return result;
  }

  public int publishOlapSchemaToServer( String schemaName, String jndiName, String modelName,
                                        boolean overwriteInRepository ) throws Exception {

    File modelsDir = new File( "models" );
    if ( !modelsDir.exists() ) {
      modelsDir.mkdir();
    }
    File publishFile;
    publishFile = new File( modelsDir, schemaName );
    publishFile.createNewFile();

    LogicalModel lModel = this.model.getLogicalModel( ModelerPerspective.ANALYSIS );

    MondrianModelExporter exporter = new MondrianModelExporter( lModel, LocalizedString.DEFAULT_LOCALE );
    String mondrianSchema = exporter.createMondrianModelXML();

    org.dom4j.Document schemaDoc = DocumentHelper.parseText( mondrianSchema );
    byte schemaBytes[] = schemaDoc.asXML().getBytes();

    if ( !publishFile.exists() ) {
      throw new ModelerException( BaseMessages.getString( PKG, "DataServicePublish.SchemaNotExist.Message" ) );
    }

    OutputStream out = new FileOutputStream( publishFile );
    out.write( schemaBytes );
    out.flush();
    out.close();

    InputStream schema = new ByteArrayInputStream( schemaBytes );

    int result = publishMondrainSchema( schema, modelName, jndiName, overwriteInRepository );

    return result;
  }

  private int publishMetaDatafile( String publishModelFileName, String domainId ) throws Exception {
    InputStream metadataFile = new FileInputStream( publishModelFileName );
    return publishMetaDataFile( metadataFile, domainId );
  }

  public void setModel( ModelerWorkspace model ) {
    this.model = model;
  }

}
