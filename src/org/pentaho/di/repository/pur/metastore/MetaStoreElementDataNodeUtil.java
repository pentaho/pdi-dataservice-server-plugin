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

package org.pentaho.di.repository.pur.metastore;

import java.util.Date;
import java.util.List;

import org.pentaho.metastore.api.IMetaStoreAttribute;
import org.pentaho.metastore.api.IMetaStoreElement;
import org.pentaho.platform.api.repository2.unified.data.node.DataNode;

public class MetaStoreElementDataNodeUtil {
  
  public DataNode copyToDataNode(IMetaStoreElement element) {
    DataNode dataNode = new DataNode(element.getName());
    
    addAttributes(element.getChildren(), dataNode);
    
    return dataNode;
  }

  private void addAttributes(List<IMetaStoreAttribute> children, DataNode dataNode) {
    for (IMetaStoreAttribute child : children) {
      if (child.getValue()==null) {
        continue; // Don't save empty values, saves space.
      }
      
      // If the child attribute doesn't have children of itself, just add it as a property to the data node
      // 
      if (child.getChildren().isEmpty()) {
        Object value = child.getValue();
        if (value instanceof Long) {
          dataNode.setProperty(child.getId(), (Long)value);
        } else if (value instanceof Date) {
          dataNode.setProperty(child.getId(), (Date)value);
        } else if (value instanceof Double) {
          dataNode.setProperty(child.getId(), (Double)value);
        } else {
          dataNode.setProperty(child.getId(), value.toString());
        }
      } else {
        // for each child attribute with additional children, we need to store them in a separate data node
        //
        DataNode childNode = new DataNode(child.getId());
        dataNode.addNode(childNode);
        addAttributes(child.getChildren(), childNode);
      }
    }
  }
  
}
