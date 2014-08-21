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

package com.pentaho.di.trans.dataservice.optimization;

import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;
import org.pentaho.metastore.persist.MetaStoreFactory;

/**
 * @author nhudak
 */
@MetaStoreElementType(
  name = "Push Down Optimization",
  description = "Define opportunities to improve Data Service performance by modifying user transformation execution"
  )
public final class PushDownOptimizationMeta {

  /**
   *  User-defined name for this optimization (required)
   */
  @MetaStoreAttribute
  private String name;

  /**
   * Name of step being optimized (optional)
   */
  @MetaStoreAttribute
  private String stepName = "";

  /**
   * Optimization Type
   */
  @MetaStoreAttribute
  private PushDownType type;

  public static MetaStoreFactory<PushDownOptimizationMeta> getMetaStoreFactory( IMetaStore metaStore, String namespace ) {
    MetaStoreFactory<PushDownOptimizationMeta> metaStoreFactory =  new MetaStoreFactory<PushDownOptimizationMeta>(
      PushDownOptimizationMeta.class, metaStore, namespace );
    PushDownOptimizationObjectFactory objectFactory = new PushDownOptimizationObjectFactory();
    metaStoreFactory.setObjectFactory( objectFactory );
    return metaStoreFactory;
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getStepName() {
    return stepName;
  }

  public void setStepName( String stepName ) {
    this.stepName = stepName;
  }

  public PushDownType getType() {
    return type;
  }

  public void setType( PushDownType type ) {
    this.type = type;
  }
}
