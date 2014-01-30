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

import java.net.ServerSocket;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Ignore;
import org.junit.Test;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.jdbc.TransDataService;
import org.pentaho.di.core.logging.LoggingObjectType;
import org.pentaho.di.core.logging.SimpleLoggingObject;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.www.Carte;
import org.pentaho.di.www.SlaveServerConfig;

public class TransDataServletTest  {

  private CarteLauncher carteLauncher;
  private Carte carte;
  private SlaveServer slaveServer;
  private DatabaseMeta databaseMeta;
  private Database database;
  
  @Test
  public void testDummy() throws Exception {
    TestCase.assertNotNull(new Object());
  }
  
  @Test
  public void test01_BasicQuery() throws Exception {
    startServer();
    try {
      database.connect();
      
      ResultSet resultSet = database.openQuery("SELECT * FROM Service");
      List<Object[]> rows = database.getRows(resultSet, 0, null);
      RowMetaInterface rowMeta = database.getReturnRowMeta();
      TestCase.assertNotNull(rowMeta);
      TestCase.assertEquals(8, rows.size());
      
      database.disconnect();
    }
    catch(Exception e) {
      e.printStackTrace();
      TestCase.fail("Unexpected exception: "+e.getLocalizedMessage());
    }
    finally {
      stopServer();
    }
  }
  
  @Test
  public void test02_NoData() throws Exception {
    startServer();
    try {
      database.connect();
      
      ResultSet resultSet = database.openQuery("SELECT * FROM Service WHERE Country = 'NoCountry'");
      List<Object[]> rows = database.getRows(resultSet, 0, null);
      RowMetaInterface rowMeta = database.getReturnRowMeta();
      TestCase.assertNotNull(rowMeta);
      TestCase.assertEquals(0, rows.size());
      
      database.disconnect();
    }
    catch(Exception e) {
      TestCase.fail("Unexpected exception: "+e.getLocalizedMessage());
    }
    finally {
      stopServer();
    }
    
  }
  
  /**
   * Test query:
   *          select "Service"."Category" as "c0", "Service"."Country" as "c1" 
   *          from "Service" as "Service" 
   *          where ((not ("Service"."Country" = 'Belgium') or ("Service"."Country" is null))) 
   *          group by "Service"."Category", "Service"."Country" 
   *          order by CASE WHEN "Service"."Category" IS NULL THEN 1 ELSE 0 END, "Service"."Category" ASC, CASE WHEN "Service"."Country" IS NULL THEN 1 ELSE 0 END, "Service"."Country" ASC
   *          
   * @throws Exception
   */
  @Test
  public void test03_MondrianQuery() throws Exception {
    startServer();
    try {
      database.connect();
      
      String query = "select \"Service\".\"Category\" as \"c0\", \"Service\".\"Country\" as \"c1\" from \"Service\" as \"Service\" where ((not (\"Service\".\"Country\" = 'Belgium') or (\"Service\".\"Country\" is null))) group by \"Service\".\"Category\", \"Service\".\"Country\" order by CASE WHEN \"Service\".\"Category\" IS NULL THEN 1 ELSE 0 END, \"Service\".\"Category\" ASC, CASE WHEN \"Service\".\"Country\" IS NULL THEN 1 ELSE 0 END, \"Service\".\"Country\" ASC"; 
      ResultSet resultSet = database.openQuery(query);
      List<Object[]> rows = database.getRows(resultSet, 0, null);
      RowMetaInterface rowMeta = database.getReturnRowMeta();
      TestCase.assertNotNull(rowMeta);
      TestCase.assertEquals(6, rows.size());
      
      database.disconnect();
    }
    catch(Exception e) {
      TestCase.fail("Unexpected exception: "+e.getLocalizedMessage());
    }
    finally {
      stopServer();
    }
  }
  
  /**
   * Test query:
   *          select "Service"."Category" as "c0", "Service"."Country" as "c1" 
   *          from "Service" as "Service" 
   *          where ((not ("Service"."Country" IN ( 'Belgium', 'Netherlands', 'Who''s the boss', 'Semicolons;Rule!') or ("Service"."Country" is null))) 
   *          group by "Service"."Category", "Service"."Country" 
   *          order by CASE WHEN "Service"."Category" IS NULL THEN 1 ELSE 0 END, "Service"."Category" ASC, CASE WHEN "Service"."Country" IS NULL THEN 1 ELSE 0 END, "Service"."Country" ASC
   *          
   * @throws Exception
   */
  @Test
  public void test03_QuotesAndSemicolons() throws Exception {
    startServer();
    try {
      database.connect();
      
      String query = "select \"Service\".\"Category\" as \"c0\", \"Service\".\"Country\" as \"c1\" from \"Service\" as \"Service\" where ((not (\"Service\".\"Country\" IN ( 'Belgium', 'Netherlands', 'Who''s the boss', 'Semicolons;Rule!')) or (\"Service\".\"Country\" is null))) group by \"Service\".\"Category\", \"Service\".\"Country\" order by CASE WHEN \"Service\".\"Category\" IS NULL THEN 1 ELSE 0 END, \"Service\".\"Category\" ASC, CASE WHEN \"Service\".\"Country\" IS NULL THEN 1 ELSE 0 END, \"Service\".\"Country\" ASC"; 
      ResultSet resultSet = database.openQuery(query);
      List<Object[]> rows = database.getRows(resultSet, 0, null);
      RowMetaInterface rowMeta = database.getReturnRowMeta();
      TestCase.assertNotNull(rowMeta);
      TestCase.assertEquals(6, rows.size());
      
      database.disconnect();
    }
    catch(Exception e) {
      TestCase.fail("Unexpected exception: "+e.getLocalizedMessage());
    }
    finally {
      stopServer();
    }
  }

  
  @Ignore
  private void startServer() throws Exception {
    KettleEnvironment.init();
    launchSlaveServer();
    databaseMeta = new DatabaseMeta("TestConnection", "KettleThin", "JDBC", slaveServer.getHostname(), "kettle", slaveServer.getPort(), "cluster", "cluster");
    databaseMeta.addExtraOption("KettleThin", "debugtrans", "/tmp/gen.ktr");
    SimpleLoggingObject loggingObject = new SimpleLoggingObject(getClass().getName(), LoggingObjectType.GENERAL, null);
    database = new Database(loggingObject, databaseMeta);
  }
  
  @Ignore
  private void stopServer() throws Exception {
    carte.getWebServer().stopServer();
  }

  @Ignore
  private CarteLauncher launchSlaveServer() throws Exception {
    ServerSocket serverSocket = new ServerSocket(0);
    int port = serverSocket.getLocalPort();
    serverSocket.close();
    
    slaveServer = new SlaveServer("test-localhost-8686-master", "127.0.0.1", Integer.toString(port), "cluster", "cluster", null, null, null, true);
    SlaveServerConfig slaveServerConfig = new SlaveServerConfig();
    slaveServerConfig.setSlaveServer(slaveServer);
    slaveServerConfig.setServices(getServicesMap());
    slaveServerConfig.setJoining(false);
    
    carteLauncher = new CarteLauncher(slaveServerConfig);
    Thread thread = new Thread(carteLauncher);
    thread.setName("Carte Launcher"+thread.getName());
    thread.start();
    // Wait until the carte object is available...
    while (carteLauncher.getCarte()==null && !carteLauncher.isFailure()) {
      Thread.sleep(100);
    }
    carte = carteLauncher.getCarte();
    
    // If there is a failure, stop the servers already launched and throw the exception
    if (carteLauncher.isFailure()) {
      if (carte!=null) {
        carte.getWebServer().stopServer();
      }
      carteLauncher.getException().printStackTrace();
      throw carteLauncher.getException(); // throw the exception for good measure.
    }
    
    return carteLauncher;
  }

  @Ignore
  private List<TransDataService> getServicesMap() {
    List<TransDataService> servicesMap = new ArrayList<TransDataService>();
    TransDataService service = new TransDataService("Service", "testfiles/sql-transmeta-test-data.ktr", null, "Output");
    servicesMap.add(service);
    return servicesMap;
  }

}
