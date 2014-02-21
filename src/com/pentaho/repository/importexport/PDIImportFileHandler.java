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

package com.pentaho.repository.importexport;

import java.util.List;

import org.pentaho.platform.api.repository2.unified.IRepositoryFileData;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.plugin.services.importer.IPlatformImportHandler;
import org.pentaho.platform.plugin.services.importer.mimeType.MimeType;
import org.pentaho.platform.plugin.services.importer.PlatformImportException;
import org.pentaho.platform.plugin.services.importer.RepositoryFileImportBundle;
import org.pentaho.platform.plugin.services.importer.RepositoryFileImportFileHandler;


public class PDIImportFileHandler extends RepositoryFileImportFileHandler implements IPlatformImportHandler {

  public PDIImportFileHandler( List<MimeType> mimeTypes ) {
    super( mimeTypes );
  }

  @Override
  protected RepositoryFile createFile( final RepositoryFileImportBundle bundle, final String repositoryPath,
      final IRepositoryFileData data ) throws PlatformImportException {

    RepositoryFileImportBundle pdiBundle = bundle;
    String originalName = bundle.getName();
    pdiBundle.setName( PDIImportUtil.checkAndSanitize( originalName ) );
    pdiBundle.setTitle( originalName );

    return super.createFile( pdiBundle, repositoryPath, data );
  }
}
