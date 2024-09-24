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

package org.pentaho.di.trans.dataservice.validation;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.paramgen.ParameterGenerationFactory;
import org.pentaho.metaverse.api.ILineageClient;
import org.pentaho.metaverse.api.MetaverseException;
import org.pentaho.metaverse.api.StepField;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.CheckStepsExtension;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.pentaho.di.trans.dataservice.validation.ValidationUtil.comment;

public class DataServiceStepValidation implements StepValidation {

  private static Class<?> PKG = DataServiceStepValidation.class;

  private final ILineageClient lineageClient;

  private final ParameterGenerationFactory serviceProvider;

  public DataServiceStepValidation( ILineageClient lineageClient,
                                    ParameterGenerationFactory serviceProvider ) {
    this.lineageClient = lineageClient;
    this.serviceProvider = serviceProvider;
  }

  @Override
  public boolean supportsStep( StepMeta step, LogChannelInterface log ) {
    return true;
  }

  @Override
  public void checkStep(
      CheckStepsExtension checkStepExtension,
      DataServiceMeta dataServiceMeta, LogChannelInterface log ) {
    if ( !isDataServiceStep( checkStepExtension.getStepMetas()[ 0 ], dataServiceMeta ) ) {
      return;
    }
    TransMeta transMeta = checkStepExtension.getTransMeta();
    String stepName = dataServiceMeta.getStepname();
    Map<String, Set<StepField>> originSteps = getOriginSteps( transMeta, stepName, log );
    if ( originSteps == null || originSteps.size() == 0 ) {
      log.logError(
          String.format(
              "Failed to retrieve originating steps for %s", stepName ) );
      return;
    }
    Map<String, Set<StepField>> unsupportedFields =
        fieldsWithStepsNotSupportingPushdown( originSteps, stepMetaMap( transMeta ) );
    warnAbout( checkStepExtension.getStepMetas()[ 0 ], unsupportedFields,
        checkStepExtension.getRemarks() );

  }

  private void warnAbout(
      StepMeta stepMeta, Map<String, Set<StepField>> unsupportedFields, List<CheckResultInterface> remarks ) {
    if ( unsupportedFields.size() == 0 ) {
      return;
    }
    Set<String> fields = unsupportedFields.keySet();
    String[] fieldStringArr = fields.toArray( new String[ fields.size() ] );
    String stepName = stepMeta.getName();
    remarks.add(
        comment(
            BaseMessages.getString( PKG,
                "DataServiceStepValidation.UnsupportedField.Message",
                stepName, Arrays.toString( fieldStringArr ) ), stepMeta ) );
  }


  private Map<String, Set<StepField>> getOriginSteps(
      TransMeta transMeta, String stepName, LogChannelInterface log ) {
    try {
      return lineageClient.getOriginSteps(
          transMeta,
          stepName,
          Arrays.asList(
              transMeta.getStepFields( stepName ).getFieldNames() )
      );
    } catch ( MetaverseException e ) {
      log.logError(
          String.format(
              "Failed to retrieve originating steps for %s", stepName ) );
    } catch ( KettleStepException e ) {
      log.logError(
          String.format(
              "Failed to retrieve step fields for %s", stepName ) );
    }
    return null;
  }

  private Map<String, StepMeta> stepMetaMap( TransMeta transMeta ) {
    return Maps.uniqueIndex(
        transMeta.getSteps(), new Function<StepMeta, String>() {
          @Override
          public String apply( StepMeta stepMeta ) {
            return stepMeta.getName();
          }
        } );
  }

  private Map<String, Set<StepField>> fieldsWithStepsNotSupportingPushdown(
      Map<String, Set<StepField>> originSteps, final Map<String, StepMeta> stepMetaMap ) {
    return Maps.filterValues(
        originSteps, new Predicate<Set<StepField>>() {
          @Override
          public boolean apply( Set<StepField> stepFields ) {
            return !stepFieldsSupportPushDown( stepFields, stepMetaMap );
          }
        }
    );
  }

  private boolean stepFieldsSupportPushDown(
      Set<StepField> stepFields, final Map<String, StepMeta> stepMetaMap ) {
    // filter out all supported steps.
    // If the resulting collection is size then no supported steps
    return Collections2.filter( stepFields,
        new Predicate<StepField>() {
          @Override
          public boolean apply( StepField stepField ) {
            return !( serviceProvider.supportsStep(
                stepMetaMap.get( stepField.getStepName() ) ) );
          }
        } ).size() == 0;
  }

  private boolean isDataServiceStep( StepMeta stepMeta, DataServiceMeta dataServiceMeta ) {
    return Const.isEmpty( stepMeta.getName() )
        || stepMeta.getName().equals( dataServiceMeta.getStepname() );
  }
}
