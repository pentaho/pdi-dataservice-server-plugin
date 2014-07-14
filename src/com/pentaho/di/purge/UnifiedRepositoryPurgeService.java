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

package com.pentaho.di.purge;

import java.io.Serializable;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.repository.pur.RootRef;
import org.pentaho.di.ui.repository.pur.services.IPurgeService;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.RepositoryRequest;
import org.pentaho.platform.api.repository2.unified.RepositoryRequest.FILES_TYPE_FILTER;
import org.pentaho.platform.api.repository2.unified.VersionSummary;
import org.pentaho.platform.repository2.unified.webservices.DefaultUnifiedRepositoryWebService;
import org.pentaho.platform.repository2.unified.webservices.RepositoryFileDto;
import org.pentaho.platform.repository2.unified.webservices.RepositoryFileTreeDto;

/**
 * Created by tkafalas 7/14/14.
 */
public class UnifiedRepositoryPurgeService implements IPurgeService {
  private final IUnifiedRepository unifiedRepository;
  private static DefaultUnifiedRepositoryWebService repoWs;
  public static final DateFormat PARAMETER_DATE_FORMAT = DateFormat.getTimeInstance( DateFormat.SHORT );
  
  public UnifiedRepositoryPurgeService(IUnifiedRepository unifiedRepository) {
    this.unifiedRepository = unifiedRepository;
  }

  @Override
  public void deleteVersionsBeforeDate( RepositoryElementInterface element, Date beforeDate ) throws KettleException {
    try {
    Serializable fileId = element.getObjectId().getId();
    deleteVersionsBeforeDate( fileId, beforeDate );
    } catch ( Exception e ) {
      processDeleteException(e);
    }
  }

  @Override
  public void deleteVersionsBeforeDate( Serializable fileId, Date beforeDate ) {
    List<VersionSummary> versionList = unifiedRepository.getVersionSummaries( fileId );
    int listSize = versionList.size();
    int removedCount = 0;
    for ( VersionSummary versionSummary : versionList ) {
      if ( listSize - removedCount >= 1 ) {
        break; //Don't delete the last instance of this file.
      }
      if ( versionSummary.getDate().before( beforeDate ) ) {
        Serializable versionId = versionSummary.getId();
        unifiedRepository.deleteFileAtVersion( fileId, versionId );
        removedCount++;
      }
    }
  }

  @Override
  public void deleteAllVersions( RepositoryElementInterface element ) throws KettleException {
    Serializable fileId = element.getObjectId().getId();
    deleteAllVersions( fileId );
  }

  @Override
  public void deleteAllVersions( Serializable fileId ) {
    keepNumberOfVersions( fileId, 0);
  }

  @Override
  public void deleteVersion( RepositoryElementInterface element, String versionId ) throws KettleException {
    String fileId = element.getObjectId().getId();
    deleteVersion( fileId, versionId );
  }

  @Override
  public void deleteVersion( Serializable fileId, Serializable versionId ) {
    unifiedRepository.deleteFileAtVersion( fileId, versionId );
  }

  @Override
  public void keepNumberOfVersions( RepositoryElementInterface element, int versionCount ) throws KettleException {
    String fileId = element.getObjectId().getId();
    keepNumberOfVersions( fileId, versionCount );
  }

  @Override
  public void keepNumberOfVersions( Serializable fileId, int versionCount ) {
    List<VersionSummary> versionList = unifiedRepository.getVersionSummaries( fileId );
    int i = 0;
    int listSize = versionList.size();
    for ( VersionSummary versionSummary : versionList ) {
      if ( i++ < listSize - versionCount ) {
        Serializable versionId = versionSummary.getId();
        unifiedRepository.deleteFileAtVersion( fileId, versionId );
      } else {
        break;
      }
    }
  }
  
  private void processDeleteException (Throwable e) throws KettleException {
    throw new KettleException( "Unable to complete revision deletion", e);
  }
  
  public void doDeleteRevisions( PurgeUtilitySpecification purgeSpecification ) throws PurgeDeletionException {
    if ( purgeSpecification != null ) {
      if ( !purgeSpecification.getPath().isEmpty() ) {
        processRevisionDeletion( purgeSpecification );
      }
      if ( purgeSpecification.isSharedObjects() ) {
        purgeSpecification.setPath( "/etc/pdi" );
        processRevisionDeletion( purgeSpecification );
      }
    }
  }

  private void processRevisionDeletion( PurgeUtilitySpecification purgeSpecification ) throws PurgeDeletionException {
    RepositoryRequest repositoryRequest = new RepositoryRequest( purgeSpecification.getPath(), true, -1, purgeSpecification.getFileFilter() );
    repositoryRequest.setTypes( FILES_TYPE_FILTER.FILES_FOLDERS );
    repositoryRequest.setIncludeMemberSet( new HashSet<String>( Arrays.asList( new String[] { "name", "id", "folder",
      "path", "versioned", "versionId", "locked" } ) ) );
    RepositoryFileTreeDto tree = getRepoWs().getTreeFromRequest( repositoryRequest );
    
    processPurgeForTree( tree, purgeSpecification );
  }
  
  private void processPurgeForTree( RepositoryFileTreeDto tree, PurgeUtilitySpecification purgeSpecification ){ 

    for ( RepositoryFileTreeDto child : tree.getChildren() ) {
      if ( !child.getChildren().isEmpty() ){
        processPurgeForTree( child, purgeSpecification );
      }
      RepositoryFileDto file = child.getFile();
      if ( file.isVersioned() ) {
        System.out.println( "checking revisions on file " + file.getPath() );
        if ( purgeSpecification.isPurgeRevisions() ) {
          deleteAllVersions( file.getId() );
        } else {
          if ( purgeSpecification.getBeforeDate() != null ) {
            deleteVersionsBeforeDate( file.getId(), purgeSpecification.getBeforeDate() );
          }
          if ( purgeSpecification.getVersionCount() >= 0 ) {
            keepNumberOfVersions( file.getId(), purgeSpecification.getVersionCount() );
          }
        }
      }
    }
  }

  public static DefaultUnifiedRepositoryWebService getRepoWs() {
    if ( repoWs == null ) {
      repoWs = new DefaultUnifiedRepositoryWebService();
    }
    return repoWs;
  }
}
