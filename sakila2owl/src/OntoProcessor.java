import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.ontology.DatatypeProperty;
import org.apache.jena.ontology.Individual;
import org.apache.jena.ontology.ObjectProperty;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.shared.JenaException;
import org.apache.jena.util.FileManager;

/*
 * @Author: Ismail Belkacim (@xd4rker)
 * 
 */

public class OntoProcessor {

	public static OntModel loadOntoFile(String ontoFile) {
		
	    OntModel oModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM, null);
	    try {
	        InputStream in = FileManager.get().open(ontoFile);
	        try {
	        	oModel.read(in, null);
	            in.close();
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	    } catch (JenaException e) {
	        System.err.println("ERROR: " + e.getMessage());
	        e.printStackTrace();
	        System.exit(0);
	    }
		
	    return oModel;
	}
	
	public static void ontoToFile(String filename, OntModel oModel) {
	    FileWriter out = null;
	    try {
	    	out = new FileWriter(filename);
	    	oModel.write(out, "RDF/XML-ABBREV");
	    } catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException ignore) {}
			}
	    }
	}
	
	public static void addDataProperty(OntModel o, Individual i, String key, Object value, XSDDatatype type) {
	    DatatypeProperty property = o.getDatatypeProperty(key);
	    i.addProperty(property, o.createTypedLiteral(value, type));
	}
	
	
	public static void addObjectProperty(OntModel o, Individual i, String key, String value) {
		ObjectProperty property = o.getObjectProperty(key);
	    i.addProperty(property, o.getIndividual(value));
	}
	
	
}
