/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.di.trans.dataservice.ui;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.pentaho.ui.xul.XulEventSourceAdapter;

import java.beans.PropertyChangeSupport;
import java.util.Map;

/**
 * @author nhudak
 */
public abstract class AbstractModel extends XulEventSourceAdapter {
  public abstract Map<String, Object> snapshot();

  public void setChangeSupport( PropertyChangeSupport changeSupport ) {
    this.changeSupport = changeSupport;
  }

  protected void firePropertyChanges( Map<String, Object> previous ) {
    Map<String, Object> update = buildSnapshot( snapshot() );
    for ( String attr : Sets.union( update.keySet(), previous.keySet() ) ) {
      firePropertyChange( attr, previous.get( attr ), update.get( attr ) );
    }
  }

  private static ImmutableMap<String, Object> buildSnapshot( Map<String, Object> snapshot ) {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for ( Map.Entry<String, Object> entry : snapshot.entrySet() ) {
      Object value = entry.getValue();
      if ( value != null ) {
        builder.put( entry );
      }
    }
    return builder.build();
  }

  protected void modify( Runnable runnable ) {
    Map<String, Object> previous = buildSnapshot( snapshot() );
    runnable.run();
    // Fire property change for all derived properties
    firePropertyChanges( previous );
  }
}
