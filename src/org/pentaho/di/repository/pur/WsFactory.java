/*!
* The Pentaho proprietary code is licensed under the terms and conditions
* of the software license agreement entered into between the entity licensing
* such code and Pentaho Corporation.
*
* This software costs money - it is not free
*
* Copyright 2002 - 2013 Pentaho Corporation.  All rights reserved.
*/

package org.pentaho.di.repository.pur;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.xml.namespace.QName;
import javax.xml.ws.BindingProvider;
import javax.xml.ws.Service;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.soap.SOAPBinding;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.util.ExecutorUtil;

import com.sun.xml.ws.developer.JAXWSProperties;

/**
 * Web service factory. Not a true factory in that the things that this factory can create are not configurable. But
 * it does cache the services.
 * 
 * @author mlowery
 */
public class WsFactory implements java.io.Serializable {

  private static final long serialVersionUID = 8992058861677534797L; /* EESOURCE: UPDATE SERIALVERUID */

  /**
   * Header name must match that specified in ProxyTrustingFilter. Note that an header has the following form: initial
   * capital letter followed by all lowercase letters.
   */
  private static final String TRUST_USER = "_trust_user_"; //$NON-NLS-1$

  private static final String NAMESPACE_URI = "http://www.pentaho.org/ws/1.0"; //$NON-NLS-1$
  
  private static final ExecutorService executor = ExecutorUtil.getExecutor();

  private static Map<String, Future<Object>> serviceCache = new HashMap<String, Future<Object>>();

  private static String lastUsername;

  @SuppressWarnings("unchecked")
  public static <T> T createService(final PurRepositoryMeta repositoryMeta, final String serviceName,
      final String username, final String password, final Class<T> clazz) throws MalformedURLException {
    final Future<Object> resultFuture;
    synchronized (serviceCache) {
      // if this is true, a coder did not make sure that clearServices was called on disconnect
      if (lastUsername != null && !lastUsername.equals(username)) {
        throw new IllegalStateException();
      } else {
        lastUsername = username;
      }

      //  build the url handling whether or not baseUrl ends with a slash
      String baseUrl = repositoryMeta.getRepositoryLocation().getUrl();
      final URL url = new URL(baseUrl + (baseUrl.endsWith("/")?"":"/")+ "webservices/" + serviceName + "?wsdl"); //$NON-NLS-1$ //$NON-NLS-2$

      String key = makeKey(url, serviceName, clazz);
      if (!serviceCache.containsKey(key)) {
        resultFuture = executor.submit(new Callable<Object>() {

          @Override
          public Object call() throws Exception {
            Service service = Service.create(url, new QName(NAMESPACE_URI, serviceName));
            T port = service.getPort(clazz);
            // add TRUST_USER if necessary
            if (StringUtils.isNotBlank(System.getProperty("pentaho.repository.client.attemptTrust"))) {
              ((BindingProvider) port).getRequestContext().put(MessageContext.HTTP_REQUEST_HEADERS,
                  Collections.singletonMap(TRUST_USER, Collections.singletonList(username)));
            } else {
              // http basic authentication
              ((BindingProvider) port).getRequestContext().put(BindingProvider.USERNAME_PROPERTY, username);
              ((BindingProvider) port).getRequestContext().put(BindingProvider.PASSWORD_PROPERTY, password);
            }
            // accept cookies to maintain session on server
            ((BindingProvider) port).getRequestContext().put(BindingProvider.SESSION_MAINTAIN_PROPERTY, true);
            // support streaming binary data
            // TODO mlowery this is not portable between JAX-WS implementations (uses com.sun)
            ((BindingProvider) port).getRequestContext().put(JAXWSProperties.HTTP_CLIENT_STREAMING_CHUNK_SIZE, 8192);
            SOAPBinding binding = (SOAPBinding) ((BindingProvider) port).getBinding();
            binding.setMTOMEnabled(true);
            return port;
          }
        });
        serviceCache.put(key, resultFuture);
      } else {
        resultFuture = serviceCache.get(key);
      }
    }
    
    try {
      return (T) resultFuture.get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause != null) {
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        } else if (cause instanceof MalformedURLException) {
          throw (MalformedURLException) cause;
        }
      }
      throw new RuntimeException(e);
    }
  }

  public synchronized static void clearServices() {
    serviceCache.clear();
    lastUsername = null;
  }

  private static String makeKey(final URL url, final String serviceName, final Class<?> clazz) {
    return url.toString() + '_' + serviceName + '_' + clazz.getName();
  }
}
