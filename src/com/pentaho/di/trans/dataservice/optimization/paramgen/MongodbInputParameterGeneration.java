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

package com.pentaho.di.trans.dataservice.optimization.paramgen;

import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationException;
import com.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
import com.pentaho.di.trans.dataservice.optimization.mongod.MongodbPredicate;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.step.StepInterface;

public class MongodbInputParameterGeneration implements ParameterGenerationService {

  private final ValueMetaResolver valueMetaResolver;

  public MongodbInputParameterGeneration( ValueMetaResolver resolver ) {
    valueMetaResolver = resolver;
  }

  public MongodbInputParameterGeneration() {
    valueMetaResolver = new ValueMetaResolver( new RowMeta() );
  }

  @Override
  public void pushDown( Condition condition, ParameterGeneration parameterGeneration, StepInterface stepInterface ) throws PushDownOptimizationException {
    if ( !"MongoDbInput".equals( stepInterface.getStepMeta().getTypeId() ) ) {
      throw new PushDownOptimizationException( "Unable to push down to type " + stepInterface.getClass() );
    }
    stepInterface.setVariable( parameterGeneration.getParameterName(),
      getMongodbPredicate( condition ).asFilterCriteria() );
  }

  @Override
  public String getParameterDefault() {
    return "{_id:{$exists:true}}";
  }


  protected MongodbPredicate getMongodbPredicate( Condition condition ) {
    return new MongodbPredicate( condition, valueMetaResolver );
  }
}
