package org.pentaho.di.repository.jcr;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.Workspace;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;

import org.apache.jackrabbit.rmi.repository.URLRemoteRepository;

public class RepTest2 {
	public static void main(String[] args) throws Exception {
		
		Repository r = new URLRemoteRepository("http://localhost:8181/jackrabbit/rmi");

		System.out.println("Repository name description = "+r.getDescriptor(Repository.REP_NAME_DESC));
		System.out.println("Spec name description = "+r.getDescriptor(Repository.SPEC_NAME_DESC));
		System.out.println("Query SQL supported = "+r.getDescriptor(Repository.OPTION_QUERY_SQL_SUPPORTED));
		System.out.println("Locking supported = "+r.getDescriptor(Repository.OPTION_LOCKING_SUPPORTED));
		System.out.println("Versioning supported = "+r.getDescriptor(Repository.OPTION_VERSIONING_SUPPORTED));
		
		Session session = r.login(new SimpleCredentials("tomcat", "tomcat".toCharArray()));
		
		try {
			Workspace workspace = session.getWorkspace();
			System.out.println("Workspace: "+workspace.getName());
			
			Node rootNode = session.getRootNode();
			printNode(rootNode, 0);
		} finally {
			session.logout();
		}
		
	}
	
	private static void printNode(Node node, int level) throws Exception {

		NodeDefinition definition = node.getDefinition();
		NodeType nodeType = definition.getDeclaringNodeType();

		boolean follow = nodeType.isNodeType("nt:unstructured") || nodeType.isNodeType("nt:folder");  
		
		boolean file = nodeType.isNodeType("nt:file"); 
		if (file) {
			PropertyIterator properties = node.getProperties();
			while (properties.hasNext()) {
				Property property = properties.nextProperty();
				indent(level);
				System.out.println("-"+property.getName());
				
				/*
				if ("jcr:data".equals(property.getName())) {
					Value value = property.getValue();
					InputStream stream = value.getStream();
					if (stream!=null) {
						String filename = "/tmp/jcr/"+node.getParent().getName();
						FileOutputStream fos = new FileOutputStream(filename);
						int c = stream.read();
						while (c>=0) {
							fos.write(c);
							c = stream.read();
						}
						fos.close();
						stream.close();
					}
				}
				*/
				System.out.println(node.getPath());
			}
		}
		
		if (!follow) {
		// 	return;
		}
		
		indent(level);
		System.out.print(node.getName());
		System.out.print("         [");
		String nodeTypeName = nodeType.getName();
		System.out.print(nodeTypeName+"]");
		System.out.println();
		
		NodeIterator nodes = node.getNodes();
		while(nodes.hasNext()) {
			Node child = nodes.nextNode();			
			printNode(child, level+1);
		}
	}
	
	private static void indent(int level) {
		for (int i=0;i<level;i++) System.out.print(" ");
	}
}
