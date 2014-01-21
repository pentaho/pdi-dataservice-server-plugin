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

import java.util.Date;

import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.platform.api.repository2.unified.data.node.DataNode;

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
          result.append('_');
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
  
  protected long getLong(DataNode node, String name) {
    if (node.hasProperty(name)) {
      return node.getProperty(name).getLong();
    } else {
      return 0L;
    }
  }
  
  protected Date getDate(DataNode node, String name) {
    if (node.hasProperty(name)) {
      return node.getProperty(name).getDate();
    } else {
      return null;
    }
  }

  protected boolean getBoolean(DataNode node, String name, boolean defaultValue) {
    if (node.hasProperty(name)) {
      return node.getProperty(name).getBoolean();
    } else {
      return defaultValue;
    }
  }
}
