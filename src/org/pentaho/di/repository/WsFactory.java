package org.pentaho.di.repository;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.ws.BindingProvider;
import javax.xml.ws.Service;
import javax.xml.ws.soap.SOAPBinding;

import org.apache.commons.codec.digest.DigestUtils;
import org.pentaho.di.repository.pur.PurRepositoryMeta;

import com.sun.xml.ws.developer.JAXWSProperties;

/**
 * Web service factory. Not a true factory in that the things that this factory can create are not configurable. But
 * it does cache the services.
 * 
 * @author mlowery
 */
public class WsFactory {

  private static final String NAMESPACE_URI = "http://www.pentaho.org/ws/1.0"; //$NON-NLS-1$

  private static Map<String, Object> serviceCache = new HashMap<String, Object>();

  @SuppressWarnings("unchecked")
  public synchronized static <T> T createService(final PurRepositoryMeta repositoryMeta, final String serviceName,
      final String username, final String password, final Class<T> clazz) throws MalformedURLException {

    URL url = new URL(repositoryMeta.getRepositoryLocation().getUrl() + "/webservices/" + serviceName + "?wsdl"); //$NON-NLS-1$ //$NON-NLS-2$

    String key = makeKey(url, serviceName, username, password, clazz);
    if (serviceCache.containsKey(key)) {
      return (T) serviceCache.get(key);
    } else {
      Service service = Service.create(url, new QName(NAMESPACE_URI, serviceName));
      T port = service.getPort(clazz);
      // http basic authentication
      ((BindingProvider) port).getRequestContext().put(BindingProvider.USERNAME_PROPERTY, username);
      ((BindingProvider) port).getRequestContext().put(BindingProvider.PASSWORD_PROPERTY, password);
      // accept cookies to maintain session on server
      ((BindingProvider) port).getRequestContext().put(BindingProvider.SESSION_MAINTAIN_PROPERTY, true);
      // support streaming binary data
      ((BindingProvider) port).getRequestContext().put(JAXWSProperties.HTTP_CLIENT_STREAMING_CHUNK_SIZE, 8192);
      SOAPBinding binding = (SOAPBinding) ((BindingProvider) port).getBinding();
      binding.setMTOMEnabled(true);

      // save this instance for later requests
      serviceCache.put(key, port);

      return port;
    }
  }

  private static String makeKey(final URL url, final String serviceName, final String username, final String password,
      final Class<?> clazz) {
    String key = url.toString() + '_' + serviceName + '_' + username + '_' + password + '_' + clazz.getName();
    String hashedKey = DigestUtils.md5Hex(key);
    return hashedKey;
  }
}
