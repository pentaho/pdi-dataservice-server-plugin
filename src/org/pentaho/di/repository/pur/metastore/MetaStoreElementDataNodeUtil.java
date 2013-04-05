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
