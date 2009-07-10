package org.pentaho.di.repository.jcr;

import java.util.Hashtable;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.jackrabbit.core.jndi.RegistryHelper;

public class RepTest {
	public static void main(String[] args) throws Exception {
		String configFile = "./repository.xml";
		String repHomeDir = "kettle";

		Hashtable<String, String> env = new Hashtable<String, String>();
				 env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.jackrabbit.core.jndi.provider.DummyInitialContextFactory");

		env.put(Context.PROVIDER_URL, "http://localhost:8181/JackRabbit/");

		InitialContext ctx = new InitialContext(env);

		RegistryHelper.registerRepository(ctx,
				 "kettle",
				 configFile,
				 repHomeDir,
				 true);

		Repository r = (Repository) ctx.lookup("kettle");

		System.out.println("Repository name description = "+r.getDescriptor(Repository.REP_NAME_DESC));
		System.out.println("Spec name description = "+r.getDescriptor(Repository.SPEC_NAME_DESC));
		System.out.println("Query SQL supported = "+r.getDescriptor(Repository.OPTION_QUERY_SQL_SUPPORTED));
		System.out.println("Locking supported = "+r.getDescriptor(Repository.OPTION_LOCKING_SUPPORTED));
		System.out.println("Versioning supported = "+r.getDescriptor(Repository.OPTION_VERSIONING_SUPPORTED));
		
		
		Session session = r.login(new SimpleCredentials("username", "password".toCharArray()));
		
		try {
			Node root = session.getRootNode();

			// Store content
			//
			Node hello = root.addNode("hello");
            Node world = hello.addNode("world");
            world.setProperty("message", "Hello, World!");
            session.save();

            // Retrieve content
            //
            Node node = root.getNode("hello/world");
            System.out.println(node.getPath());
            System.out.println(node.getProperty("message").getString());

            // Remove content
            //
			root.getNode("hello").remove();
			session.save();
			
		} finally {
			session.logout();
		}
		
	}
}
