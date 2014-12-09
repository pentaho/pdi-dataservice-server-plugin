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

import com.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.agilebi.modeler.ModelerException;
import org.pentaho.agilebi.modeler.ModelerWorkspace;
import org.pentaho.agilebi.modeler.util.ModelerWorkspaceUtil;
import org.pentaho.agilebi.modeler.util.TableModelerSource;
import org.pentaho.di.core.database.DatabaseMeta;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class PublishHelper {

  private static final String TMP_DIR_NAME = "tmp";
  private static final String XMI_EXTENSION = ".xmi";
  private static final String MONDRIAN_XML = ".mondrian.xml";

  public static int publish( ModelerWorkspace modelerWorkspace, BaServerConnection baServerConnection,
                             DatabaseMeta databaseMeta, DataServiceMeta dataServiceMeta ) throws Exception {

    File file = generateAutoModel( modelerWorkspace, databaseMeta, dataServiceMeta );

    int result;

    ModelServerPublish modelServerPublish = new ModelServerPublish( modelerWorkspace, baServerConnection );
    result = modelServerPublish.publishToServer( dataServiceMeta.getName() + MONDRIAN_XML, databaseMeta.getName(),
      dataServiceMeta.getName(), file.getAbsolutePath() );

    if ( result == ModelServerPublish.PUBLISH_SUCCESS ) {
      if ( !modelServerPublish.publishDataSource( databaseMeta ) ) {
        result = ModelServerPublish.PUBLISH_FAILED;
      }
    }

    return result;
  }

  public static File generateAutoModel( ModelerWorkspace modelerWorkspace, DatabaseMeta databaseMeta,
                                        DataServiceMeta dataServiceMeta ) throws ModelerException,
    IOException {

    TableModelerSource tableModelerSource = new TableModelerSource( databaseMeta, dataServiceMeta.getName(), null );
    ModelerWorkspaceUtil.populateModelFromSource( modelerWorkspace, tableModelerSource );
    modelerWorkspace.setSourceName( dataServiceMeta.getName() );

    modelerWorkspace.getWorkspaceHelper().autoModelFlat( modelerWorkspace );
    modelerWorkspace.getWorkspaceHelper().autoModelRelationalFlat( modelerWorkspace );

    String xmi = ModelerWorkspaceUtil.getMetadataXML( modelerWorkspace );

    File tempDir = new File( TMP_DIR_NAME );
    if ( !tempDir.exists() ) {
      tempDir.mkdir();
    }

    File file = new File( tempDir, modelerWorkspace.getModelName() + XMI_EXTENSION );
    BufferedWriter writer = new BufferedWriter( new FileWriter( file.getAbsoluteFile() ) );
    writer.write( xmi );
    writer.close();

    return file;
  }

  public static int testConnection( BaServerConnection baServerConnection ) {

    ModelServerPublish modelServerPublish = new ModelServerPublish( baServerConnection );
    return modelServerPublish.testConnection();

  }

  public static String getBaServerCompatibleDatabaseName(String name) {
    if (name == null) {
      return null;
    }
    // replace spaces with underscores
    return name.replaceAll("\\s", "_");
  }

}
