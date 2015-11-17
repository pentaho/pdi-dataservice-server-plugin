/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
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
