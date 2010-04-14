package org.pentaho.di.repository.pur;

import java.util.Date;

import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;

import com.pentaho.repository.pur.data.node.DataNode;

public abstract class AbstractDelegate {

  protected static final String PROP_NAME = "NAME"; //$NON-NLS-1$

  protected static final String PROP_DESCRIPTION = "DESCRIPTION"; //$NON-NLS-1$

  protected LogChannelInterface log;
  
  public AbstractDelegate() {
    log = LogChannel.GENERAL;
  }

  protected String sanitizeNodeName(final String name) {
    StringBuffer result = new StringBuffer(30);

    for (char c : name.toCharArray()) {
      switch (c) {
        case ':':
        case '/':
          result.append('-');
          break;
        case '{':
        case '}':
        case '[':
        case ']':
        case ')':
        case '(':
        case '\\':
          result.append(' ');
          break;
        default:
          if (Character.isLetterOrDigit(c)) {
            result.append(c);
          }
          break;
      }
    }

    return result.toString();
  }
  
  protected String getString(DataNode node, String name) {
    if (node.hasProperty(name)) {
      return node.getProperty(name).getString();
    } else {
      return null;
    }
  }
  
  protected Date getDate(DataNode node, String name) {
    if (node.hasProperty(name)) {
      return node.getProperty(name).getDate();
    } else {
      return null;
    }
  }

}
