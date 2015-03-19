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

package com.pentaho.di.trans.dataservice.validation;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.pentaho.di.trans.dataservice.DataServiceMeta;
import com.pentaho.di.trans.dataservice.DataServiceMetaStoreUtil;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import com.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGeneration;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.Collection;

public class ValidationUtil {

  public static DataServiceMeta getDataServiceMeta(
      TransMeta transMeta, IMetaStore metaStore, LogChannelInterface log ) {
    if ( metaStore == null || transMeta == null ) {
      log.logBasic(
          String.format( "Unable to determine whether '%s' is associated with a DataService.",
              transMeta == null ? "(unknown)" : transMeta.getName() ) );
      return null;
    }
    try {
      return DataServiceMetaStoreUtil.fromTransMeta( transMeta, metaStore );
    } catch ( MetaStoreException e ) {
      log.logError(
          String.format(
              "Error while attempting to load DataServiceMeta during step validation for '%s'.",
              transMeta.getName() ),
          e );
    }
    return null;
  }

  public static Collection<ParameterGeneration> getParameterGenerationsForStep(
      final DataServiceMeta dataServiceMeta, final String stepName ) {
    return Collections2.transform(
        Collections2.filter( dataServiceMeta.getPushDownOptimizationMeta(), new Predicate<PushDownOptimizationMeta>() {
              @Override
              public boolean apply( PushDownOptimizationMeta pushDownOptimizationMeta ) {
                return pushDownOptimizationMeta.getType() instanceof ParameterGeneration
                    && stepName.equals( pushDownOptimizationMeta.getStepName() );
              }
            }
        ),
        new Function<PushDownOptimizationMeta, ParameterGeneration>() {
          @Override
          public ParameterGeneration apply( PushDownOptimizationMeta pushDownOptimizationMeta ) {
            return (ParameterGeneration) pushDownOptimizationMeta.getType();
          }
        } );
  }

  public static boolean paramSubstitutionModifiesString( String string, VariableSpace space ) {
    return !space.environmentSubstitute( string ).equals( string );
  }

  public static CheckResult warn( String msg, StepMeta stepMeta ) {
    return new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, msg, stepMeta );
  }

  public static CheckResult comment( String msg, StepMeta stepMeta ) {
    return new CheckResult( CheckResultInterface.TYPE_RESULT_COMMENT, msg, stepMeta );
  }

}
