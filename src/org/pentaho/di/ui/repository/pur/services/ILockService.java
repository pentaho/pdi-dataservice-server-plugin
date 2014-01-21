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

import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.IRepositoryService;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.pur.model.RepositoryLock;
/**
 * Repository service which adds a locking service to the repository. Using this feature, 
 * the user of the repository can lock, unlock a particular object in the repository
 * @author rmansoor
 *
 */
public interface ILockService extends IRepositoryService{
  
  /**
   * Locks this job for exclusive use by the current user of the repository
   * @param id_job the id of the job to lock 
   * @param message the lock message
   * 
   * @return Repository lock object
   * @throws KettleException in case something goes wrong or the job is already locked by someone else.
   */
  public RepositoryLock lockJob(ObjectId id_job, String message) throws KettleException;
  
  /**
   * Unlocks a job, allowing other people to modify it again.
   * @param id_job the id of the transformation to unlock
   * @throws KettleException in case something goes wrong with the database or connection
   */
  public void unlockJob(ObjectId id_job) throws KettleException;

  /**
   * Return the lock object for this job.  Returns null if there is no lock present.
   * 
   * @param id_job
   * @return the lock object for this job, null if no lock is present.
   * @throws KettleDatabaseException
   */
  public RepositoryLock getJobLock(ObjectId id_job) throws KettleException;
  
  /**
   * Locks this transformation for exclusive use by the current user of the repository
   * @param id_transformation the id of the transformation to lock
   * @param isSessionScoped If isSessionScoped is true then this lock will expire upon the expiration of the current session (either through an automatic or explicit Session.logout); if false, this lock does not expire until explicitly unlocked or automatically unlocked due to a implementation-specific limitation, such as a timeout. 
   * @param message the lock message
   * 
   * @return Transformation lock
   * @throws KettleException in case something goes wrong or the transformation is already locked by someone else.
   */
  public RepositoryLock lockTransformation(ObjectId id_transformation, String message) throws KettleException;

  /**
   * Unlocks a transformation, allowing other people to modify it again.
   * @param id_transformation the id of the transformation to unlock
   * @throws KettleException in case something goes wrong with the database or connection
   */
  public void unlockTransformation(ObjectId id_transformation) throws KettleException;

  /**
   * Return the lock object for this transformation.  Returns null if there is no lock present.
   * 
   * @param id_transformation
   * @return the lock object for this transformation, null if no lock is present.
   * @throws KettleException in case something goes wrong with the database or connection
   */
  public RepositoryLock getTransformationLock(ObjectId id_transformation) throws KettleException;

  /**
   * Return true if the file can be unlocked by the logged in user
   * @param id
   * @return
   * @throws KettleException
   */
  public boolean canUnlockFileById(final ObjectId id) throws KettleException;
}
