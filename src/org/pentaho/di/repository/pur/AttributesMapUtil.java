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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.pentaho.di.core.AttributesInterface;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.platform.api.repository2.unified.data.node.DataNode;
import org.pentaho.platform.api.repository2.unified.data.node.DataProperty;

public class AttributesMapUtil {
  
  public static final String NODE_ATTRIBUTE_GROUPS = "ATTRIBUTE_GROUPS";
  
  public static final void saveAttributesMap(DataNode dataNode, AttributesInterface attributesInterface) throws KettleException {
    Map<String, Map<String, String>> attributesMap = attributesInterface.getAttributesMap();
    
    DataNode attributeNodes = dataNode.getNode(NODE_ATTRIBUTE_GROUPS);
    if (attributeNodes == null) {
      attributeNodes = dataNode.addNode(NODE_ATTRIBUTE_GROUPS);
    }
    for (String groupName : attributesMap.keySet()) {
      DataNode attributeNode = attributeNodes.getNode(groupName);
      if (attributeNode == null) {
        attributeNode = attributeNodes.addNode(groupName);
      }
      Map<String, String> attributes = attributesMap.get(groupName);
      for (String key : attributes.keySet()) {
        String value = attributes.get(key);
        if (key!=null && value!=null) {
          attributeNode.setProperty(key, attributes.get(key));
        }
      }
    }    
  }
  
  public static final void loadAttributesMap(DataNode dataNode, AttributesInterface attributesInterface) throws KettleException {
    Map<String, Map<String, String>> attributesMap = new HashMap<String, Map<String,String>>();
    attributesInterface.setAttributesMap(attributesMap);
    
    DataNode groupsNode = dataNode.getNode(NODE_ATTRIBUTE_GROUPS);
    if (groupsNode!=null) {
      Iterable<DataNode> nodes = groupsNode.getNodes();
      for (Iterator<DataNode> groupsIterator=nodes.iterator();groupsIterator.hasNext();) {
        DataNode groupNode = groupsIterator.next();
        HashMap<String, String> attributes = new HashMap<String, String>();
        attributesMap.put(groupNode.getName(), attributes);
        Iterable<DataProperty> properties = groupNode.getProperties();
        for (Iterator<DataProperty> propertiesIterator = properties.iterator();propertiesIterator.hasNext();) {
          DataProperty dataProperty = propertiesIterator.next();
          String key = dataProperty.getName();
          String value = dataProperty.getString();
          if (key!=null && value!=null) {
            attributes.put(key, value);
          }
        }
      }
    }
  }

}
