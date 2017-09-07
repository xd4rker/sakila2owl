import java.util.ArrayList;
import java.util.HashMap;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.ontology.Individual;
import org.apache.jena.ontology.OntClass;
import org.apache.jena.ontology.OntModel;

import org.apache.log4j.BasicConfigurator;

/*
 * @Author: Ismail Belkacim (@xd4rker)
 * 
 */

public class Main {
	
	public static void main(String[] args) {
				
		DBQuery dbquery = new DBQuery("jdbc:mysql://localhost/sakila?useSSL=false", "root", "toor");
		
		String ontoFile = "E:/sakila.owl";
		
		// Configure the logger
		BasicConfigurator.configure();
		
		System.out.println("[+] Loading Ontology " + ontoFile);

		// Loading the ontology
	    OntModel oModel = OntoProcessor.loadOntoFile(ontoFile);
	    
	    System.out.println("[+] Ontology " + ontoFile + " loaded.");
	    
	    // Get ontology namespace
	    String namespace = oModel.getNsPrefixURI("");
	    
		// Fetch Countries from DB
		ArrayList<HashMap<String, Object>> countries = dbquery.getCountries();

	    // Populate Country Class
	    System.out.println("[+] Populating Country Class");
	    OntClass countryClass = oModel.getOntClass(namespace + "Country");
	    
	    for(HashMap<String, Object> country : countries) {
		    Individual C = oModel.createIndividual(namespace + "COUNTRY_" + Integer.toString((int) country.get("country_id")), countryClass);
		    OntoProcessor.addDataProperty(oModel, C, namespace + "country_id", country.get("country_id"), XSDDatatype.XSDint);
		    OntoProcessor.addDataProperty(oModel, C, namespace + "country", country.get("country"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, C, namespace + "last_update", country.get("last_update"), XSDDatatype.XSDdateTimeStamp);
	    }
	    
	    // No longer needed, let GC do its work
	    countries = null;
	    
	    // Fetch Cities from DB
		ArrayList<HashMap<String, Object>> cities = dbquery.getCities();
		
	    // Populate City Class
		System.out.println("[+] Populating City Class");
	    OntClass cityClass = oModel.getOntClass(namespace + "City");
	    
	    for(HashMap<String, Object> city : cities) {
		    Individual CI = oModel.createIndividual(namespace + "CITY_" + Integer.toString((int) city.get("city_id")), cityClass);
		    OntoProcessor.addDataProperty(oModel, CI, namespace + "city_id", city.get("city_id"), XSDDatatype.XSDint);
		    OntoProcessor.addDataProperty(oModel, CI, namespace + "city", city.get("city"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, CI, namespace + "last_update", city.get("last_update"), XSDDatatype.XSDdateTimeStamp);
		    
		    OntoProcessor.addObjectProperty(oModel, CI, namespace + "belongsToCountry", namespace + "COUNTRY_" + city.get("country_id"));
	    }
	    
	    cities = null;

	   // Fetch Addresses from DB
		ArrayList<HashMap<String, Object>> addresses = dbquery.getAddresses();

	    // Populate Address Class
		System.out.println("[+] Populating Address Class");
	    OntClass addressClass = oModel.getOntClass(namespace + "Address");
	    
	    for(HashMap<String, Object> address : addresses) {
		    Individual ADDR = oModel.createIndividual(namespace + "ADDRESS_" + Integer.toString((int) address.get("address_id")), addressClass);
		    OntoProcessor.addDataProperty(oModel, ADDR, namespace + "address_id", address.get("address_id"), XSDDatatype.XSDint);
		    OntoProcessor.addDataProperty(oModel, ADDR, namespace + "address", address.get("address"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, ADDR, namespace + "address2", address.get("address2"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, ADDR, namespace + "district", address.get("district"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, ADDR, namespace + "postal_code", address.get("postal_code"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, ADDR, namespace + "phone", address.get("phone"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, ADDR, namespace + "location", address.get("location"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, ADDR, namespace + "last_update", address.get("last_update"), XSDDatatype.XSDdateTimeStamp);
		    
		    OntoProcessor.addObjectProperty(oModel, ADDR, namespace + "belongsToCity", namespace + "CITY_" + address.get("city_id"));
	    }
	    
	    addresses = null;
	    
	   // Fetch Staffs from DB
		ArrayList<HashMap<String, Object>> staffs = dbquery.getStaffs();

	    // Populate Staff Class
		System.out.println("[+] Populating Staff Class");
	    OntClass staffClass = oModel.getOntClass(namespace + "Staff");
	    
	    for(HashMap<String, Object> staff : staffs) {
		    Individual STF = oModel.createIndividual(namespace + "STAFF_" + Integer.toString((int) staff.get("staff_id")), staffClass);
		    OntoProcessor.addDataProperty(oModel, STF, namespace + "staff_id", staff.get("staff_id"), XSDDatatype.XSDint);
		    OntoProcessor.addDataProperty(oModel, STF, namespace + "first_name", staff.get("first_name"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, STF, namespace + "last_name", staff.get("last_name"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, STF, namespace + "email", staff.get("email"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, STF, namespace + "active", (Boolean) staff.get("active"), XSDDatatype.XSDboolean);
		    OntoProcessor.addDataProperty(oModel, STF, namespace + "username", staff.get("username"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, STF, namespace + "password", staff.get("password"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, STF, namespace + "picture",staff.get("picture"), XSDDatatype.XSDbase64Binary);
		    OntoProcessor.addDataProperty(oModel, STF, namespace + "last_update", staff.get("last_update"), XSDDatatype.XSDdateTimeStamp);
		    
		    OntoProcessor.addObjectProperty(oModel, STF, namespace + "hasAddress", namespace + "ADDRESS_" + staff.get("address_id"));
	    }
	    
	    staffs = null;
	    
	   // Fetch Stores from DB
		ArrayList<HashMap<String, Object>> stores = dbquery.getStores();
	
	    // Populate Store Class
		System.out.println("[+] Populating Store Class");
	    OntClass storeClass = oModel.getOntClass(namespace + "Store");
	    
	    for(HashMap<String, Object> store : stores) {
		    Individual STR = oModel.createIndividual(namespace + "STORE_" + Integer.toString((int) store.get("store_id")), storeClass);
		    OntoProcessor.addDataProperty(oModel, STR, namespace + "store_id", store.get("store_id"), XSDDatatype.XSDint);
		    OntoProcessor.addDataProperty(oModel, STR, namespace + "last_update", store.get("last_update"), XSDDatatype.XSDdateTimeStamp);
		    
		    OntoProcessor.addObjectProperty(oModel, STR, namespace + "hasAddress", namespace + "ADDRESS_" + store.get("address_id"));
		    OntoProcessor.addObjectProperty(oModel, STR, namespace + "hasManager", namespace + "STAFF_" + store.get("manager_staff_id"));
		    OntoProcessor.addObjectProperty(oModel, oModel.getIndividual(namespace + "STAFF_" + store.get("manager_staff_id")), namespace + "managesStore", namespace + "STORE_" + store.get("store_id"));
	    }
	    
	    stores = null;	    
	    
	    // Fetch Customers from DB
		ArrayList<HashMap<String, Object>> customers = dbquery.getCustomers();
	
	    // Populate Customer Class
		System.out.println("[+] Populating Customer Class");
	    OntClass customerClass = oModel.getOntClass(namespace + "Customer");
	    
	    for(HashMap<String, Object> customer : customers) {
		    Individual CUST = oModel.createIndividual(namespace + "CUSTOMER_" + Integer.toString((int) customer.get("customer_id")), customerClass);
		    OntoProcessor.addDataProperty(oModel, CUST, namespace + "customer_id", customer.get("customer_id"), XSDDatatype.XSDint);
		    OntoProcessor.addDataProperty(oModel, CUST, namespace + "first_name", customer.get("first_name"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, CUST, namespace + "last_name", customer.get("last_name"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, CUST, namespace + "email", customer.get("email"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, CUST, namespace + "active", customer.get("active"), XSDDatatype.XSDboolean);
		    OntoProcessor.addDataProperty(oModel, CUST, namespace + "create_date", customer.get("create_date"), XSDDatatype.XSDdateTimeStamp);
		    OntoProcessor.addDataProperty(oModel, CUST, namespace + "last_update", customer.get("last_update"), XSDDatatype.XSDdateTimeStamp);
		    
		    OntoProcessor.addObjectProperty(oModel, CUST, namespace + "hasAddress", namespace + "ADDRESS_" + customer.get("address_id"));
		    OntoProcessor.addObjectProperty(oModel, CUST, namespace + "belongsToStore", namespace + "STORE_" + customer.get("store_id"));
	    }
	    
	    customers = null;	
	    
	    // Fetch Languages from DB
		ArrayList<HashMap<String, Object>> languages = dbquery.getLanguages();
	
	    // Populate Language Class
		System.out.println("[+] Populating Language Class");
	    OntClass languageClass = oModel.getOntClass(namespace + "Language");
	    
	    for(HashMap<String, Object> language : languages) {
		    Individual CUST = oModel.createIndividual(namespace + "LANGUAGE_" + Integer.toString((int) language.get("language_id")), languageClass);
		    OntoProcessor.addDataProperty(oModel, CUST, namespace + "language_id", language.get("language_id"), XSDDatatype.XSDint);
		    OntoProcessor.addDataProperty(oModel, CUST, namespace + "name", language.get("name"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, CUST, namespace + "last_update", language.get("last_update"), XSDDatatype.XSDdateTimeStamp);
	    }
	    
	    languages = null;

	    // Fetch Films from DB
		ArrayList<HashMap<String, Object>> films = dbquery.getFilms();
	
	    // Populate Film Class
		System.out.println("[+] Populating Film Class");
	    OntClass filmClass = oModel.getOntClass(namespace + "Film");
	    
	    for(HashMap<String, Object> film : films) {
		    Individual FLM = oModel.createIndividual(namespace + "FILM_" + Integer.toString((int) film.get("film_id")), filmClass);
		    OntoProcessor.addDataProperty(oModel, FLM, namespace + "film_id", film.get("film_id"), XSDDatatype.XSDint);
		    OntoProcessor.addDataProperty(oModel, FLM, namespace + "title", film.get("title"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, FLM, namespace + "description", film.get("description"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, FLM, namespace + "release_year", film.get("release_year"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, FLM, namespace + "rental_duration", film.get("rental_duration"), XSDDatatype.XSDint);
		    OntoProcessor.addDataProperty(oModel, FLM, namespace + "rental_rate", film.get("rental_rate"), XSDDatatype.XSDfloat);
		    OntoProcessor.addDataProperty(oModel, FLM, namespace + "length", film.get("length"), XSDDatatype.XSDint);
		    OntoProcessor.addDataProperty(oModel, FLM, namespace + "replacement_cost", film.get("replacement_cost"), XSDDatatype.XSDfloat);
		    OntoProcessor.addDataProperty(oModel, FLM, namespace + "rating", film.get("rating"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, FLM, namespace + "special_features", film.get("special_features"), XSDDatatype.XSDstring);		    
		    OntoProcessor.addDataProperty(oModel, FLM, namespace + "last_update", film.get("last_update"), XSDDatatype.XSDdateTimeStamp);
		    
		    OntoProcessor.addObjectProperty(oModel, FLM, namespace + "hasLanguage", namespace + "LANGUAGE_" + film.get("language_id"));
		    if((int) film.get("original_language_id") != 0) {
			    OntoProcessor.addObjectProperty(oModel, FLM, namespace + "hasOriginalLanguage", namespace + "LANGUAGE_" + film.get("original_language_id"));
		    }
	    }
	    
	    films = null;
	    
	    // Fetch Inventories from DB
		ArrayList<HashMap<String, Object>> inventories = dbquery.getInventories();
	
	    // Populate Inventory Class
		System.out.println("[+] Populating Inventory Class");
	    OntClass inventoryClass = oModel.getOntClass(namespace + "Inventory");
	    
	    for(HashMap<String, Object> inventorie : inventories) {
		    Individual INV = oModel.createIndividual(namespace + "INVENTORY_" + Integer.toString((int) inventorie.get("inventory_id")), inventoryClass);
		    OntoProcessor.addDataProperty(oModel, INV, namespace + "inventory_id", inventorie.get("inventory_id"), XSDDatatype.XSDint);
		    OntoProcessor.addDataProperty(oModel, INV, namespace + "last_update", inventorie.get("last_update"), XSDDatatype.XSDdateTimeStamp);
		    
		    OntoProcessor.addObjectProperty(oModel, INV, namespace + "belongsToFilm", namespace + "FILM_" + inventorie.get("film_id"));
		    OntoProcessor.addObjectProperty(oModel, INV, namespace + "belongsToStore", namespace + "STORE_" + inventorie.get("store_id"));
	    }
	    
	    inventories = null;
	    
	    // Fetch Rentals from DB
		ArrayList<HashMap<String, Object>> rentals = dbquery.getRentals();
	
	    // Populate Rental Class
		System.out.println("[+] Populating Rental Class");
	    OntClass rentalClass = oModel.getOntClass(namespace + "Rental");
	    
	    for(HashMap<String, Object> rental : rentals) {
		    Individual RENT = oModel.createIndividual(namespace + "RENTAL_" + Integer.toString((int) rental.get("rental_id")), rentalClass);
		    OntoProcessor.addDataProperty(oModel, RENT, namespace + "rental_id", rental.get("rental_id"), XSDDatatype.XSDint);
		    OntoProcessor.addDataProperty(oModel, RENT, namespace + "rental_date", rental.get("rental_date"), XSDDatatype.XSDdateTimeStamp);
		    OntoProcessor.addDataProperty(oModel, RENT, namespace + "return_date", rental.get("return_date"), XSDDatatype.XSDdateTimeStamp);
		    OntoProcessor.addDataProperty(oModel, RENT, namespace + "last_update", rental.get("last_update"), XSDDatatype.XSDdateTimeStamp);
		    
		    OntoProcessor.addObjectProperty(oModel, RENT, namespace + "belongsToInventory", namespace + "INVENTORY_" + rental.get("inventory_id"));
		    OntoProcessor.addObjectProperty(oModel, RENT, namespace + "belongsToCustomer", namespace + "CUSTOMER_" + rental.get("customer_id"));
		    OntoProcessor.addObjectProperty(oModel, RENT, namespace + "hasManager", namespace + "STAFF_" + rental.get("staff_id"));
	    }
	    
	    rentals = null;
	    
	    // Fetch Payments from DB
		ArrayList<HashMap<String, Object>> payments = dbquery.getPayments();
	
	    // Populate Payment Class
		System.out.println("[+] Populating Payment Class");
	    OntClass paymentClass = oModel.getOntClass(namespace + "Payment");
	    
	    for(HashMap<String, Object> payment : payments) {
		    Individual PAYMT = oModel.createIndividual(namespace + "PAYMENT_" + Integer.toString((int) payment.get("payment_id")), paymentClass);
		    OntoProcessor.addDataProperty(oModel, PAYMT, namespace + "payment_id", payment.get("payment_id"), XSDDatatype.XSDint);
		    OntoProcessor.addDataProperty(oModel, PAYMT, namespace + "amount", payment.get("amount"), XSDDatatype.XSDfloat);
		    OntoProcessor.addDataProperty(oModel, PAYMT, namespace + "payment_date", payment.get("payment_date"), XSDDatatype.XSDdateTimeStamp);
		    OntoProcessor.addDataProperty(oModel, PAYMT, namespace + "last_update", payment.get("last_update"), XSDDatatype.XSDdateTimeStamp);
		    
		    OntoProcessor.addObjectProperty(oModel, PAYMT, namespace + "belongsToCustomer", namespace + "CUSTOMER_" + payment.get("customer_id"));
		    OntoProcessor.addObjectProperty(oModel, PAYMT, namespace + "hasManager", namespace + "STAFF_" + payment.get("staff_id"));
		    
		    if((int) payment.get("rental_id") != 0) {
		    	OntoProcessor.addObjectProperty(oModel, PAYMT, namespace + "belongsToRental", namespace + "RENTAL_" + payment.get("rental_id"));
		    }
	    }
	    
	    payments = null;
	    
	    // Fetch Categories from DB
		ArrayList<HashMap<String, Object>> categories = dbquery.getCategories();
	
	    // Populate Category Class
		System.out.println("[+] Populating Category Class");
	    OntClass categoryClass = oModel.getOntClass(namespace + "Category");
	    
	    for(HashMap<String, Object> categorie : categories) {
		    Individual CAT = oModel.createIndividual(namespace + "CATEGORY_" + Integer.toString((int) categorie.get("category_id")), categoryClass);
		    OntoProcessor.addDataProperty(oModel, CAT, namespace + "category_id", categorie.get("category_id"), XSDDatatype.XSDint);
		    OntoProcessor.addDataProperty(oModel, CAT, namespace + "name", categorie.get("name"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, CAT, namespace + "last_update", categorie.get("last_update"), XSDDatatype.XSDdateTimeStamp);
	    }
	    
	    categories = null;
	    
	    // Fetch FilmsCategories from DB
		ArrayList<HashMap<String, Object>> filmCategories = dbquery.getFilmCategories();
		
	    // Populate FilmCategory Class
		System.out.println("[+] Populating FilmCategory Class");
	    OntClass filmCategoryClass = oModel.getOntClass(namespace + "FilmCategory");
	    
	    for(HashMap<String, Object> filmCategorie : filmCategories) {
		    Individual FCAT = oModel.createIndividual(namespace + "FILM_CATEGORY_" + Integer.toString((int) filmCategorie.get("film_id")) + "_" + Integer.toString((int) filmCategorie.get("category_id")), filmCategoryClass);
		    OntoProcessor.addDataProperty(oModel, FCAT, namespace + "last_update", filmCategorie.get("last_update"), XSDDatatype.XSDdateTimeStamp);
		    OntoProcessor.addObjectProperty(oModel, FCAT, namespace + "belongsToFilm", namespace + "FILM_" + filmCategorie.get("film_id"));
		    OntoProcessor.addObjectProperty(oModel, FCAT, namespace + "belongsToCategory", namespace + "CATEGORY_" + filmCategorie.get("category_id"));
	    }
	    
	    filmCategories = null;
	    
	    // Fetch Actors from DB
		ArrayList<HashMap<String, Object>> actors = dbquery.getActors();
		
	    // Populate Actor Class
		System.out.println("[+] Populating Actor Class");
	    OntClass actorClass = oModel.getOntClass(namespace + "Actor");
	    
	    for(HashMap<String, Object> actor : actors) {
		    Individual ACT = oModel.createIndividual(namespace + "ACTOR_" + Integer.toString((int) actor.get("actor_id")), actorClass);
		    OntoProcessor.addDataProperty(oModel, ACT, namespace + "actor_id", actor.get("actor_id"), XSDDatatype.XSDint);
		    OntoProcessor.addDataProperty(oModel, ACT, namespace + "first_name", actor.get("first_name"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, ACT, namespace + "last_name", actor.get("last_name"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, ACT, namespace + "last_update", actor.get("last_update"), XSDDatatype.XSDdateTimeStamp);
	    }
	    
	    actors = null;
	    
	    // Fetch FilmActors from DB
		ArrayList<HashMap<String, Object>> filmActors = dbquery.getFilmActors();
		
	    // Populate FilmActor Class
		System.out.println("[+] Populating FilmActor Class");
	    OntClass filmActorsClass = oModel.getOntClass(namespace + "FilmActor");
	    
	    for(HashMap<String, Object> filmActor : filmActors) {
		    Individual FACT = oModel.createIndividual(namespace + "FILM_ACTOR_" + Integer.toString((int) filmActor.get("film_id")) + "_" + Integer.toString((int) filmActor.get("actor_id")), filmActorsClass);
		    OntoProcessor.addDataProperty(oModel, FACT, namespace + "last_update", filmActor.get("last_update"), XSDDatatype.XSDdateTimeStamp);
		    OntoProcessor.addObjectProperty(oModel, FACT, namespace + "belongsToFilm", namespace + "FILM_" + filmActor.get("film_id"));
		    OntoProcessor.addObjectProperty(oModel, FACT, namespace + "belongsToActor", namespace + "ACTOR_" + filmActor.get("actor_id"));
	    }
	    
	    filmActors = null;
	    
	    // Fetch FilmTexts from DB
		ArrayList<HashMap<String, Object>> filmTexts = dbquery.getFilmTexts();
		
	    // Populate FilmText Class
		System.out.println("[+] Populating FilmText Class");
	    OntClass filmTextClass = oModel.getOntClass(namespace + "FilmText");
	    
	    for(HashMap<String, Object> filmText : filmTexts) {
		    Individual FTXT = oModel.createIndividual(namespace + "FILM_TEXT_" + Integer.toString((int) filmText.get("film_id")), filmTextClass);
		    OntoProcessor.addDataProperty(oModel, FTXT, namespace + "film_id", filmText.get("film_id"), XSDDatatype.XSDint);
		    OntoProcessor.addDataProperty(oModel, FTXT, namespace + "title", filmText.get("title"), XSDDatatype.XSDstring);
		    OntoProcessor.addDataProperty(oModel, FTXT, namespace + "description", filmText.get("description"), XSDDatatype.XSDstring);
	    }
	    
	    filmTexts = null;
	    
	    System.out.println("[+] Saving to new ontology");
	    
	    // Save populated ontology to new file
	    OntoProcessor.ontoToFile("E:/sakila-populated.owl", oModel);
	    
	    System.out.println("[+] Done");

		
	}

}
