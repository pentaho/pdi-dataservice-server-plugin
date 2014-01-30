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

package org.pentaho.di.repository.pur;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.ObjectRevision;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.ui.repository.pur.services.IRevisionService;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.VersionSummary;

public class UnifiedRepositoryRevisionService implements IRevisionService {
  private final IUnifiedRepository unifiedRepository;
  private final RootRef rootRef;
  
  public UnifiedRepositoryRevisionService(IUnifiedRepository unifiedRepository, RootRef rootRef) {
    this.unifiedRepository = unifiedRepository;
    this.rootRef = rootRef;
  }

  @Override
  public List<ObjectRevision> getRevisions(final RepositoryElementInterface element) throws KettleException {
    return getRevisions(element.getObjectId());
  }

  @Override
  public List<ObjectRevision> getRevisions( ObjectId fileId ) throws KettleException {
    String absPath = null;
    try {
      List<ObjectRevision> versions = new ArrayList<ObjectRevision>();
      List<VersionSummary> versionSummaries = unifiedRepository.getVersionSummaries(fileId.getId());
      for (VersionSummary versionSummary : versionSummaries) {
        versions.add(new PurObjectRevision(versionSummary.getId(), versionSummary.getAuthor(),
            versionSummary.getDate(), versionSummary.getMessage()));
      }
      return versions;
    } catch (Exception e) {
      throw new KettleException("Could not retrieve version history of object with path [" + absPath + "]", e);
    }
  }

  @Override
  public void restoreJob( ObjectId id_job, String revision, String versionComment ) throws KettleException {
    unifiedRepository.restoreFileAtVersion(id_job.getId(), revision, versionComment);
    rootRef.clearRef();
  }

  @Override
  public void restoreTransformation( ObjectId id_transformation, String revision, String versionComment )
    throws KettleException {
    unifiedRepository.restoreFileAtVersion(id_transformation.getId(), revision, versionComment);
    rootRef.clearRef();
  }

}
