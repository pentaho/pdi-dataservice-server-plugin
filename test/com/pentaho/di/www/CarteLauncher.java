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

package com.pentaho.di.www;

import org.pentaho.di.www.Carte;
import org.pentaho.di.www.SlaveServerConfig;

public class CarteLauncher implements Runnable {
  private SlaveServerConfig config;
  private Carte             carte;
  private Exception         exception;
  private boolean           failure;

  public CarteLauncher() {
    this.carte = null;
  }
  
  public CarteLauncher(String hostname, int port) {
    this();
    this.config = new SlaveServerConfig(hostname, port, false);
  }
  
  public CarteLauncher(SlaveServerConfig config) {
    this();
    this.config = config;
  }

  public void run() {
    try {
      carte = new Carte(config);
    } catch (Exception e) {
      this.exception = e;
      failure = true;
    }
  }

  /**
   * @return the carte
   */
  public Carte getCarte() {
    return carte;
  }

  /**
   * @param carte
   *          the carte to set
   */
  public void setCarte(Carte carte) {
    this.carte = carte;
  }

  /**
   * @return the exception
   */
  public Exception getException() {
    return exception;
  }

  /**
   * @param exception
   *          the exception to set
   */
  public void setException(Exception exception) {
    this.exception = exception;
  }

  /**
   * @return the failure
   */
  public boolean isFailure() {
    return failure;
  }

  /**
   * @param failure
   *          the failure to set
   */
  public void setFailure(boolean failure) {
    this.failure = failure;
  }

  /**
   * @return the config
   */
  public SlaveServerConfig getConfig() {
    return config;
  }

  /**
   * @param config the config to set
   */
  public void setConfig(SlaveServerConfig config) {
    this.config = config;
  }
}
