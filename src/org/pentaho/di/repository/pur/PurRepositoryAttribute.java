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

import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositoryAttributeInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.platform.api.repository2.unified.data.node.DataNode;
import org.pentaho.platform.api.repository2.unified.data.node.DataProperty;

public class PurRepositoryAttribute implements RepositoryAttributeInterface, java.io.Serializable {

  private static final long serialVersionUID = -5787096049770518000L; /* EESOURCE: UPDATE SERIALVERUID */

	private DataNode	dataNode;
	private List<DatabaseMeta>	databases;

	public PurRepositoryAttribute(DataNode dataNode, List<DatabaseMeta> databases) {
		this.dataNode = dataNode;
		this.databases = databases;
	}

	public void setAttribute(String code, String value) {
		dataNode.setProperty(code, value);
	}

	public String getAttributeString(String code) {
		DataProperty property = dataNode.getProperty(code);
		if (property!=null) {
			return property.getString();
		}
		return null;
	}

	public void setAttribute(String code, boolean value) {
		dataNode.setProperty(code, value);
	}

	public boolean getAttributeBoolean(String code) {
		DataProperty property = dataNode.getProperty(code);
		if (property!=null) {
			return property.getBoolean();
		}
		return false;
	}

	public void setAttribute(String code, long value) {
		dataNode.setProperty(code, value);
	}

	public long getAttributeInteger(String code) {
		DataProperty property = dataNode.getProperty(code);
		if (property!=null) {
			return property.getLong();
		}
		return 0L;
	}

	public void setAttribute(String code, DatabaseMeta databaseMeta) {
		dataNode.setProperty(code, databaseMeta.getObjectId().getId());
	}

	public DatabaseMeta getAttributeDatabaseMeta(String code) {
		DataProperty property =  dataNode.getProperty(code);
		if (property==null || Const.isEmpty(property.getString())) {
			return null;
		}
		ObjectId id = new StringObjectId(property.getString());
		return DatabaseMeta.findDatabase(databases, id);
	}
}
