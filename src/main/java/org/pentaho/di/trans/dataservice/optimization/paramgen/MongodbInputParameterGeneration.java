/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.optimization.paramgen;

import org.pentaho.di.trans.dataservice.optimization.OptimizationImpactInfo;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationException;
import org.pentaho.di.trans.dataservice.optimization.ValueMetaResolver;
import org.pentaho.di.trans.dataservice.optimization.mongod.MongodbPredicate;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInput;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;
import org.pentaho.mongo.wrapper.field.MongoField;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

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
      getMongodbPredicate( condition, getFieldMappings( stepInterface ) ).asFilterCriteria() );
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

      String predicate = getMongodbPredicate( pushDownCondition,  getFieldMappings( stepInterface ) ).asFilterCriteria();
      String modifiedQuery = parameterGeneration.setQueryParameter( jsonQuery, predicate );
      if ( !modifiedQuery.equals( jsonQuery ) ) {
        impactInfo.setQueryAfterOptimization( modifiedQuery );
        impactInfo.setModified( true );
      }
    } catch ( KettleException e ) {
      log.logDetailed( String.format( "Unable to optimize step '%s'", stepInterface.getStepname() ) );
      impactInfo.setModified( false );
      impactInfo.setErrorMsg( e );
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

  protected Map<String, String> getFieldMappings( StepInterface stepInterface ) {
    Map<String, String> fieldMap = new HashMap<String, String>();

    MongoDbInput mongoDbInput = (MongoDbInput) stepInterface;
    MongoDbInputMeta mongoDbInputMeta = (MongoDbInputMeta) mongoDbInput.getStepMeta().getStepMetaInterface();
    for ( MongoField mongoField : mongoDbInputMeta.getMongoFields() ) {
      fieldMap.put( mongoField.getName(), mongoField.getPath() );
    }

    return fieldMap;
  }

  private void logFailedToGetJson( String xml, Exception e ) {
    log.logError( "Failed to read json_query from xml:  " + xml, e );
  }

  protected MongodbPredicate getMongodbPredicate( Condition condition, Map<String, String> fieldMappings ) {
    return new MongodbPredicate( condition, valueMetaResolver, fieldMappings );
  }
}
