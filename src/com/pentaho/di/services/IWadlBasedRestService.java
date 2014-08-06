package com.pentaho.di.services;

import java.net.URI;
import java.util.Map;

import com.sun.jersey.api.client.Client;

public interface IWadlBasedRestService {
  
  IWadlBasedRestService getService( String serviceClassName, Client client, URI baseURI);
  
  IWadlBasedRestService getService( String serviceClassName, Client client);
  
  Object runServiceWithOutput( IWadlBasedRestService restService, Map<String, Object> paramaterMap );
}
