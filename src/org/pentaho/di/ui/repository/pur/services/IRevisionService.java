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

package org.pentaho.di.ui.repository.pur.services;

import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.IRepositoryService;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.ObjectRevision;
import org.pentaho.di.repository.RepositoryElementInterface;
/**
 * Repository service which adds a revision feature to the repository. Using this feature,
 * user of this repository can get revisions of the object and restore to a specific version
 * @author rmansoor
 *
 */
public interface IRevisionService extends IRepositoryService{

  /**
   * Get the revision history of a repository element.
   * 
   * @param element the element.  If the ID is specified, this will be taken.  Otherwise it will be looked up.
   * 
   * @return The revision history, sorted from first to last.
   * @throws KettleException in case something goes horribly wrong
   */
   public List<ObjectRevision> getRevisions(RepositoryElementInterface element) throws KettleException;

  /**
   * Get the revision history of a repository element.
   * 
   * @param element the element.  If the ID is specified, this will be taken.  Otherwise it will be looked up.
   * 
   * @return The revision history, sorted from first to last.
   * @throws KettleException in case something goes horribly wrong
   */
  public List<ObjectRevision> getRevisions(ObjectId id) throws KettleException;
  
  /**
   * Restore a job from the given revision. The state of the specified revision becomes
   * the current / latest state of the job.
   * @param id_job id of the job
   * @param revision revision to restore
   * @throws KettleException
   */
  public void restoreJob(ObjectId id_job, String revision, String versionComment) throws KettleException;
  
  /**
   * Restore a transformation from the given revision. The state of the specified revision becomes
   * the current / latest state of the transformation.
   * @param id_transformation id of the transformation
   * @param revision revision to restore
   * @throws KettleException
   */
  public void restoreTransformation(ObjectId id_transformation, String revision, String versionComment) throws KettleException;

}
