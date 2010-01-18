package org.pentaho.di.repository.pur;

import org.pentaho.di.core.logging.LogChannel;

public abstract class AbstractDelegate {

  protected static final String PROP_NAME = "NAME"; //$NON-NLS-1$

  protected static final String PROP_DESCRIPTION = "DESCRIPTION"; //$NON-NLS-1$

  protected LogChannel log;
  
  public AbstractDelegate() {
    log = new LogChannel(this);
  }

  protected String sanitizeNodeName(final String name) {
    StringBuffer result = new StringBuffer(30);

    for (char c : name.toCharArray()) {
      switch (c) {
        case ':':
          result.append('-');
          break;
        case '/':
          result.append("-");
          break;
        default:
          result.append(c);
          break;
      }
    }

    return result.toString();
  }
}
