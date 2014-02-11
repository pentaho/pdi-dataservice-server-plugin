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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Serializable;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.repository.pur.JobDelegate;
import org.pentaho.platform.api.repository2.unified.Converter;
import org.pentaho.platform.api.repository2.unified.IRepositoryFileData;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.api.repository2.unified.data.node.NodeRepositoryFileData;
import org.w3c.dom.Document;

/**
 * Converts stream of binary or character data.
 * 
 * @author mlowery
 */
public class StreamToJobNodeConverter implements Converter {

  IUnifiedRepository unifiedRepository;

  /**
   * 
   * @param unifiedRepository
   */
  public StreamToJobNodeConverter( IUnifiedRepository unifiedRepository ) {
    this.unifiedRepository = unifiedRepository;
  }

  /**
   * 
   * @param data
   * @return
   */
  public InputStream convert( final IRepositoryFileData data ) {
    throw new UnsupportedOperationException();
  }

  /**
   * 
   * @param fileId
   * @return
   */
  public InputStream convert( final Serializable fileId ) {
    InputStream is = null;

    try {
      if ( fileId != null ) {
        Repository repository = PDIImportUtil.connectToRepository( null );
        RepositoryFile file = unifiedRepository.getFileById( fileId );
        if ( file != null ) {
          JobMeta jobMeta = repository.loadJob( new StringObjectId( fileId.toString() ), null );
          if ( jobMeta != null ) {
            return new ByteArrayInputStream( jobMeta.getXML().getBytes() );
          }
        }
      }
    } catch ( Exception e ) {
      e.printStackTrace();
    }
    return is;
  }

  /**
   * 
   * @param inputStream
   * @param charset
   * @param mimeType
   * @return
   */
  public IRepositoryFileData convert( final InputStream inputStream, final String charset, final String mimeType ) {
    try {
      long size = inputStream.available();
      JobMeta jobMeta = new JobMeta();
      Repository repository = PDIImportUtil.connectToRepository( null );
      Document doc = PDIImportUtil.loadXMLFrom( inputStream );
      if ( doc != null ) {
        jobMeta.loadXML( doc.getDocumentElement(), repository, null );
        JobDelegate delegate = new JobDelegate( repository );
        delegate.saveSharedObjects( jobMeta, null );
        return new NodeRepositoryFileData(delegate.elementToDataNode( jobMeta ), size );
      } else {
        return null;
      }
    } catch ( Exception e ) {
      return null;
    }
  }

  public void saveSharedObjects( final Repository repo, final RepositoryElementInterface element )
    throws KettleException {
    JobMeta jobMeta = (JobMeta) element;
    // First store the databases and other depending objects in the transformation.
    for ( DatabaseMeta databaseMeta : jobMeta.getDatabases() ) {
      if ( databaseMeta.getObjectId() == null || !StringUtils.isEmpty( databaseMeta.getHostname() ) ) {
        repo.save( databaseMeta, null, null );
      }
    }

    // Store the slave servers...
    //
    for ( SlaveServer slaveServer : jobMeta.getSlaveServers() ) {
      if (slaveServer.getObjectId() == null ) {
        repo.save( slaveServer, null, null );
      }
    }

  }
}
