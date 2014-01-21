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

package org.pentaho.di.repository.pur.model;

import java.util.Date;

import org.pentaho.di.repository.ObjectId;

public class RepositoryLock implements java.io.Serializable {

  private static final long serialVersionUID = -5107186539466626808L; /* EESOURCE: UPDATE SERIALVERUID */
	private ObjectId objectId;
	private String message;
	private String login;
	private String username;
	private Date   lockDate;
	
	/**
	 * Create a new repository lock object for the current date/time.
	 * 
	 * @param objectId
	 * @param login
	 * @param message
	 * @param username
	 */
	public RepositoryLock(ObjectId objectId, String message, String login, String username) {
		this(objectId, message, login, username, new Date()); // now
	}	

	/**
	 * Create a new repository lock object.
	 *  
	 * @param objectId
	 * @param message
	 * @param username
	 * @param lockDate
	 */
	public RepositoryLock(ObjectId objectId, String message, String login, String username, Date lockDate) {
		this.objectId = objectId;
		this.message = message;
		this.login = login;
		this.username = username;
		this.lockDate = lockDate;
	}	

	/**
	 * @return the objectId
	 */
	public ObjectId getObjectId() {
		return objectId;
	}

	/**
	 * @param objectId the objectId to set
	 */
	public void setObjectId(ObjectId objectId) {
		this.objectId = objectId;
	}

	/**
	 * @return the message
	 */
	public String getMessage() {
		return message;
	}

	/**
	 * @param message the message to set
	 */
	public void setMessage(String message) {
		this.message = message;
	}

	/**
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * @param username the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * @return the lockDate
	 */
	public Date getLockDate() {
		return lockDate;
	}

	/**
	 * @param lockDate the lockDate to set
	 */
	public void setLockDate(Date lockDate) {
		this.lockDate = lockDate;
	}

	/**
	 * @return the login
	 */
	public String getLogin() {
		return login;
	}

	/**
	 * @param login the login to set
	 */
	public void setLogin(String login) {
		this.login = login;
	}
	
	
}
