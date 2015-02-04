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

import com.pentaho.di.trans.dataservice.optimization.OptimizationImpactInfo;
import com.pentaho.di.trans.dataservice.optimization.PushDownOptimizationException;
import com.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
import com.pentaho.di.trans.dataservice.optimization.mongod.MongodbPredicate;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;

public class MongodbInputParameterGeneration implements ParameterGenerationService {

  private final ValueMetaResolver valueMetaResolver;

  protected LogChannelInterface log = new LogChannel( this );

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

  @Override
  public OptimizationImpactInfo preview( Condition pushDownCondition,
                                         ParameterGeneration parameterGeneration, StepInterface stepInterface ) {
    OptimizationImpactInfo impactInfo = new OptimizationImpactInfo( stepInterface.getStepname() );
    try {
      String jsonQuery = getJsonQuery( stepInterface );
      impactInfo.setQueryBeforeOptimization( jsonQuery );

      if ( pushDownCondition == null ) {
        impactInfo.setModified( false );
        return impactInfo;
      }

      String predicate = getMongodbPredicate( pushDownCondition ).asFilterCriteria();
      String modifiedQuery = parameterGeneration.setQueryParameter( jsonQuery, predicate );
      if ( !modifiedQuery.equals( jsonQuery ) ) {
        impactInfo.setQueryAfterOptimization( modifiedQuery );
        impactInfo.setModified( true );
      }
    } catch ( KettleException e ) {
      log.logDetailed( String.format( "Unable to optimize step '%s'", stepInterface.getStepname() ) );
      impactInfo.setModified( false );
      impactInfo.setErrorMsg( e.getMessage() );
    }
    return impactInfo;
  }

  private String getJsonQuery( StepInterface stepInterface ) throws KettleException {
    String xml = stepInterface.getStepMeta().getXML();
    DocumentBuilder builder;
    String jsonQuery = "";
    try {
      builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = builder.parse( new InputSource( new StringReader( xml ) ) );
      NodeList nodes = doc.getElementsByTagName( "json_query" );
      if ( nodes.getLength() > 0 ) {
        jsonQuery = nodes.item( 0 ).getTextContent();
      }
    } catch ( ParserConfigurationException e ) {
      logFailedToGetJson( xml, e );
    } catch ( SAXException e ) {
      logFailedToGetJson( xml, e );
    } catch ( IOException e ) {
      logFailedToGetJson( xml, e );
    }
    return jsonQuery;
  }

  private void logFailedToGetJson( String xml, Exception e ) {
    log.logError( "Failed to read json_query from xml:  " + xml, e );
  }

  protected MongodbPredicate getMongodbPredicate( Condition condition ) {
    return new MongodbPredicate( condition, valueMetaResolver );
  }
}
