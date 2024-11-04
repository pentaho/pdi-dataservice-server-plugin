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


package org.pentaho.di.trans.dataservice.ui.model;

import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownType;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.ui.xul.XulEventSourceAdapter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DataServiceModel extends XulEventSourceAdapter {

  private List<PushDownOptimizationMeta> pushDownOptimizations = Lists.newArrayList();
  private String serviceName;
  private String serviceStep;
  private int serviceMaxRows;
  private long serviceMaxTime;
  private boolean streaming;
  private final TransMeta transMeta;

  public DataServiceModel( TransMeta transMeta ) {
    this( transMeta, false );
  }

  public DataServiceModel( TransMeta transMeta, boolean streaming ) {
    this.transMeta = transMeta;
    this.streaming = streaming;
  }

  public TransMeta getTransMeta() {
    return transMeta;
  }

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName( String serviceName ) {
    String previous = this.serviceName;
    this.serviceName = serviceName;
    firePropertyChange( "serviceName", previous, serviceName );
  }

  public int getServiceMaxRows() {
    return serviceMaxRows;
  }

  public void setServiceMaxRows( int serviceMaxRows ) {
    int previous = this.serviceMaxRows;
    this.serviceMaxRows = serviceMaxRows;
    firePropertyChange( "serviceMaxRows", previous, serviceMaxRows );
  }

  public long getServiceMaxTime() {
    return serviceMaxTime;
  }

  public void setServiceMaxTime( long serviceMaxTime ) {
    long previous = this.serviceMaxTime;
    this.serviceMaxTime = serviceMaxTime;
    firePropertyChange( "serviceMaxTime", previous, serviceMaxTime );
  }

  public void setStreaming( boolean streaming ) {
    this.streaming = streaming;
  }

  public boolean isStreaming() {
    return this.streaming;
  }

  public String getServiceStep() {
    return serviceStep;
  }

  public void setServiceStep( String serviceStep ) {
    String previous = this.serviceStep;
    this.serviceStep = serviceStep;
    firePropertyChange( "serviceStep", previous, serviceStep );
  }

  public ImmutableList<String> getStepFields() {
    List<String> fields = new ArrayList<>();
    if ( getTransMeta() != null && getServiceStep() != null ) {
      StepMeta stepMeta = transMeta.findStep( serviceStep );
      try {
        RowMetaInterface row = transMeta.getStepFields( stepMeta );
        for ( int i = 0; i < row.size(); i++ ) {
          fields.add( row.getValueMeta( i ).getName() );
        }
        return ImmutableList.copyOf( fields );
      } catch ( KettleException e ) {
        // Do nothing for now
      }
    }

    return ImmutableList.of();
  }

  public ImmutableList<PushDownOptimizationMeta> getPushDownOptimizations() {
    return ImmutableList.copyOf( pushDownOptimizations );
  }

  public void setPushDownOptimizations( List<PushDownOptimizationMeta> pushDownOptimizations ) {
    ImmutableList<PushDownOptimizationMeta> previous = getPushDownOptimizations();
    this.pushDownOptimizations = Lists.newArrayList( pushDownOptimizations );

    firePropertyChange( "pushDownOptimizations", previous, getPushDownOptimizations() );
  }

  public boolean add( PushDownOptimizationMeta pushDownOptimizationMeta ) {
    return addAll( ImmutableList.of( pushDownOptimizationMeta ) );
  }

  public boolean addAll( Collection<PushDownOptimizationMeta> pushDownOptimizations ) {
    ImmutableList<PushDownOptimizationMeta> previous = getPushDownOptimizations();

    if ( this.pushDownOptimizations.addAll( pushDownOptimizations ) ) {
      firePropertyChange( "pushDownOptimizations", previous, getPushDownOptimizations() );
      return true;
    }

    return false;
  }

  public boolean remove( PushDownOptimizationMeta pushDownOptimization ) {
    ImmutableList<PushDownOptimizationMeta> previous = getPushDownOptimizations();

    if ( this.pushDownOptimizations.remove( pushDownOptimization ) ) {
      firePropertyChange( "pushDownOptimizations", previous, getPushDownOptimizations() );
      return true;
    }

    return false;
  }

  public boolean removeAll( Collection<PushDownOptimizationMeta> c ) {
    ImmutableList<PushDownOptimizationMeta> previous = getPushDownOptimizations();

    if ( pushDownOptimizations.removeAll( c ) ) {
      firePropertyChange( "pushDownOptimizations", previous, getPushDownOptimizations() );
      return true;
    }

    return false;
  }

  public DataServiceMeta getDataService() {
    DataServiceMeta dataService = new DataServiceMeta( transMeta );
    dataService.setName( getServiceName() );
    dataService.setPushDownOptimizationMeta( getPushDownOptimizations() );
    dataService.setStepname( getServiceStep() );
    dataService.setStreaming( isStreaming() );
    dataService.setRowLimit( serviceMaxRows );
    dataService.setTimeLimit( serviceMaxTime );

    for ( PushDownOptimizationMeta pushDownOptimization : pushDownOptimizations ) {
      pushDownOptimization.getType().init( transMeta, dataService, pushDownOptimization );
    }

    return dataService;
  }

  public ImmutableList<PushDownOptimizationMeta> getPushDownOptimizations( final Class<? extends PushDownType> type ) {
    return FluentIterable.from( getPushDownOptimizations() )
      .filter( new Predicate<PushDownOptimizationMeta>() {
        @Override public boolean apply( PushDownOptimizationMeta input ) {
          return type.isInstance( input.getType() );
        }
      } )
      .toList();
  }
}
