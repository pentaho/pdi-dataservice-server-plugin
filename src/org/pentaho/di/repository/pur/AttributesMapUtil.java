/*!
* The Pentaho proprietary code is licensed under the terms and conditions
* of the software license agreement entered into between the entity licensing
* such code and Pentaho Corporation.
*
* This software costs money - it is not free
*
* Copyright 2002 - 2013 Pentaho Corporation.  All rights reserved.
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
