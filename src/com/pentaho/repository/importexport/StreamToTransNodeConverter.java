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
import org.pentaho.di.cluster.ClusterSchema;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.partition.PartitionSchema;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.repository.pur.TransDelegate;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.platform.api.repository2.unified.Converter;
import org.pentaho.platform.api.repository2.unified.IRepositoryFileData;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.api.repository2.unified.data.node.NodeRepositoryFileData;
import org.w3c.dom.Document;

/**
 * Converts stream of binary or character data.
 * 
 * @author rmansoor
 */
public class StreamToTransNodeConverter implements Converter {
  IUnifiedRepository unifiedRepository;

  public StreamToTransNodeConverter( IUnifiedRepository unifiedRepository ) {
    this.unifiedRepository = unifiedRepository;
  }

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
      // this will change in the future if PDI no longer has its
      // own repository. For now, get the reference
      if ( fileId != null ) {
        Repository repository = PDIImportUtil.connectToRepository( null );
        RepositoryFile file = unifiedRepository.getFileById( fileId );
        if ( file != null ) {
          TransMeta transMeta = repository.loadTransformation( new StringObjectId( fileId.toString() ), null );
          if ( transMeta != null ) {
            return new ByteArrayInputStream( transMeta.getXML().getBytes() );
          }
        }
      }
    } catch ( Exception e ) {
      e.printStackTrace();
    }
    return is;
  }

  public IRepositoryFileData convert( final InputStream inputStream, final String charset, final String mimeType ) {
    try {
      TransMeta transMeta = new TransMeta();
      Repository repository = PDIImportUtil.connectToRepository( null );
      Document doc = PDIImportUtil.loadXMLFrom( inputStream );
      transMeta.loadXML( doc.getDocumentElement(), repository, false );
      TransDelegate delegate = new TransDelegate( repository );
      saveSharedObjects( repository, transMeta );
      return new NodeRepositoryFileData( delegate.elementToDataNode( transMeta ) );
    } catch ( Exception e ) {
      e.printStackTrace();
      return null;
    }
  }

  private void saveSharedObjects( final Repository repo, final RepositoryElementInterface element )
    throws KettleException {
    TransMeta transMeta = (TransMeta) element;
    // First store the databases and other depending objects in the transformation.
    for ( DatabaseMeta databaseMeta : transMeta.getDatabases() ) {
        if ( databaseMeta.getObjectId() == null || !StringUtils.isEmpty( databaseMeta.getHostname() ) ) {
          repo.save( databaseMeta, null, null );
        }
    }

    // Store the slave servers...
    //
    for ( SlaveServer slaveServer : transMeta.getSlaveServers() ) {
      if ( slaveServer.hasChanged() || slaveServer.getObjectId() == null ) {
        repo.save( slaveServer, null, null );
      }
    }

    // Store the cluster schemas
    //
    for ( ClusterSchema clusterSchema : transMeta.getClusterSchemas() ) {
      if ( clusterSchema.hasChanged() || clusterSchema.getObjectId() == null ) {
        repo.save( clusterSchema, null, null );
      }
    }

    // Save the partition schemas
    //
    for ( PartitionSchema partitionSchema : transMeta.getPartitionSchemas() ) {
      if ( partitionSchema.hasChanged() || partitionSchema.getObjectId() == null ) {
        repo.save( partitionSchema, null, null );
      }
    }
  }

}
