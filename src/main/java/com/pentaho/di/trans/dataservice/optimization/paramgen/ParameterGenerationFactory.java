/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2015 Pentaho Corporation (Pentaho). All rights reserved.
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

package com.pentaho.di.trans.dataservice.optimization.paramgen;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.pentaho.di.trans.dataservice.optimization.PushDownFactory;
import com.pentaho.di.trans.dataservice.optimization.PushDownType;
import org.pentaho.di.trans.step.StepMeta;

import java.util.List;

/**
 * @author nhudak
 */
public class ParameterGenerationFactory implements PushDownFactory {

  private final List<ParameterGenerationServiceFactory> factories;

  public ParameterGenerationFactory( List<ParameterGenerationServiceFactory> factories ) {
    this.factories = factories;
  }

  @Override public String getName() {
    return ParameterGeneration.TYPE_NAME;
  }

  @Override public Class<? extends PushDownType> getType() {
    return ParameterGeneration.class;
  }

  @Override public ParameterGeneration createPushDown() {
    return new ParameterGeneration( this );
  }

  @Override public ParamGenOptForm createPushDownOptTypeForm() {
    return new ParamGenOptForm( this );
  }

  public ParameterGenerationService getService( StepMeta stepMeta ) {
    Optional<ParameterGenerationServiceFactory> factory = getFactory( stepMeta );
    return factory.isPresent() ? factory.get().getService( stepMeta ) : null;
  }

  public boolean supportsStep( StepMeta stepMeta ) {
    return getFactory( stepMeta ).isPresent();
  }

  private Optional<ParameterGenerationServiceFactory> getFactory( final StepMeta stepMeta ) {
    return Iterables.tryFind( factories, new Predicate<ParameterGenerationServiceFactory>() {
      @Override public boolean apply( ParameterGenerationServiceFactory input ) {
        return input.supportsStep( stepMeta );
      }
    } );
  }
}
