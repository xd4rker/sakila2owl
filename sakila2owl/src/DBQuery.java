import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;

import org.apache.commons.io.IOUtils;

/*
 * @Author: Ismail Belkacim (@xd4rker)
 * 
 */

public class DBQuery {

	private String dbUrl;
	private String dbUser;
	private String dbPassword;
	private Connection conx = null;

	public DBQuery(String url, String user, String password) {
		dbUrl = url;
		dbUser = user;
		dbPassword = password;	
		connectDB();
	}
	
	private void connectDB() {
		try {
	        conx = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	}
	
	public ArrayList<HashMap<String, Object>> getCountries() {
        ArrayList<HashMap<String, Object>> resList = new ArrayList<HashMap<String, Object>>();
		try {
	        Statement stmt = null;
	        String query = "SELECT *, DATE_FORMAT(last_update, '%Y-%m-%dT%TZ') as last_update_formatted FROM country";
	        stmt = conx.createStatement();
	        ResultSet rs = stmt.executeQuery(query);
	        while (rs.next()) {
	            HashMap<String, Object> row = new HashMap<String, Object>();
	            row.put("country_id", rs.getInt("country_id"));
	            row.put("country", rs.getString("country"));
	            row.put("last_update", rs.getString("last_update_formatted"));
	            resList.add(row);
	        }
		} catch(Exception e) {
			e.printStackTrace();
		}
		return resList;
	}
	
	public ArrayList<HashMap<String, Object>> getCities() {
        ArrayList<HashMap<String, Object>> resList = new ArrayList<HashMap<String, Object>>();
		try {
	        Statement stmt = null;
	        String query = "SELECT *, DATE_FORMAT(last_update, '%Y-%m-%dT%TZ') as last_update_formatted FROM city";
	        stmt = conx.createStatement();
	        ResultSet rs = stmt.executeQuery(query);
	        while (rs.next()) {
	            HashMap<String, Object> row = new HashMap<String, Object>();
	            row.put("city_id", rs.getInt("city_id"));
	            row.put("city", rs.getString("city"));
	            row.put("country_id", rs.getInt("country_id"));
	            row.put("last_update", rs.getString("last_update_formatted"));
	            resList.add(row);
	        }
		} catch(Exception e) {
			e.printStackTrace();
		}
		return resList;
	}
	
	public ArrayList<HashMap<String, Object>> getAddresses() {
        ArrayList<HashMap<String, Object>> resList = new ArrayList<HashMap<String, Object>>();
		try {
	        Statement stmt = null;
	        String query = "SELECT *, DATE_FORMAT(last_update, '%Y-%m-%dT%TZ') as last_update_formatted ,AsText(location) as location_data FROM address";
	        stmt = conx.createStatement();
	        ResultSet rs = stmt.executeQuery(query);
	        while (rs.next()) {
	            HashMap<String, Object> row = new HashMap<String, Object>();
	            row.put("address_id", rs.getInt("address_id"));
	            row.put("address", rs.getString("address"));
	            row.put("address2", (rs.getString("address2") != null) ? rs.getString("address2") : "null");
	            row.put("district", rs.getString("district"));
	            row.put("city_id", rs.getInt("city_id"));
	            row.put("postal_code", (rs.getString("postal_code") != null) ? rs.getString("postal_code") : "null");
	            row.put("phone", rs.getString("phone"));
	            row.put("location", IOUtils.toString(rs.getBinaryStream("location_data"), StandardCharsets.UTF_8));
	            row.put("last_update", rs.getString("last_update_formatted"));
	            resList.add(row);
	        }
		} catch(Exception e) {
			e.printStackTrace();
		}
		return resList;
	}
	
	public ArrayList<HashMap<String, Object>> getStaffs() {
        ArrayList<HashMap<String, Object>> resList = new ArrayList<HashMap<String, Object>>();
		try {
	        Statement stmt = null;
	        String query = "SELECT *, DATE_FORMAT(last_update, '%Y-%m-%dT%TZ') as last_update_formatted FROM staff";
	        stmt = conx.createStatement();
	        ResultSet rs = stmt.executeQuery(query);
	        while (rs.next()) {
	            HashMap<String, Object> row = new HashMap<String, Object>();
	            row.put("staff_id", rs.getInt("staff_id"));
	            row.put("first_name", rs.getString("first_name"));
	            row.put("last_name", rs.getString("last_name"));
	            row.put("email", (rs.getString("email") != null) ? rs.getString("email") : "null");
	            row.put("active", rs.getBoolean("active"));
	            row.put("address_id", rs.getInt("address_id"));
	            row.put("username", rs.getString("username"));
	            row.put("password", (rs.getString("password") != null) ? rs.getString("password") : "null");
	            row.put("last_update", rs.getString("last_update_formatted"));
	            
	            InputStream rawpic = rs.getBinaryStream("picture");
	            if(rawpic != null) {
	            	row.put("picture", Base64.getEncoder().encodeToString(IOUtils.toByteArray(rawpic)));
	            } else {
	            	row.put("picture", "null");
	            }
	            	            
	            resList.add(row);
	        }
		} catch(Exception e) {
			e.printStackTrace();
		}
		return resList;
	}
	
	public ArrayList<HashMap<String, Object>> getStores() {
        ArrayList<HashMap<String, Object>> resList = new ArrayList<HashMap<String, Object>>();
		try {
	        Statement stmt = null;
	        String query = "SELECT *, DATE_FORMAT(last_update, '%Y-%m-%dT%TZ') as last_update_formatted FROM store";
	        stmt = conx.createStatement();
	        ResultSet rs = stmt.executeQuery(query);
	        while (rs.next()) {
	            HashMap<String, Object> row = new HashMap<String, Object>();
	            row.put("store_id", rs.getInt("store_id"));
	            row.put("manager_staff_id", rs.getInt("manager_staff_id"));
	            row.put("address_id", rs.getInt("address_id"));
	            row.put("last_update", rs.getString("last_update_formatted"));
	            resList.add(row);
	        }
		} catch(Exception e) {
			e.printStackTrace();
		}
		return resList;
	}
	
	public ArrayList<HashMap<String, Object>> getCustomers() {
        ArrayList<HashMap<String, Object>> resList = new ArrayList<HashMap<String, Object>>();
		try {
	        Statement stmt = null;
	        String query = "SELECT *, DATE_FORMAT(create_date, '%Y-%m-%dT%TZ') as create_date_formatted, DATE_FORMAT(last_update, '%Y-%m-%dT%TZ') as last_update_formatted FROM customer";
	        stmt = conx.createStatement();
	        ResultSet rs = stmt.executeQuery(query);
	        while (rs.next()) {
	            HashMap<String, Object> row = new HashMap<String, Object>();
	            row.put("customer_id", rs.getInt("customer_id"));
	            row.put("store_id", rs.getInt("store_id"));
	            row.put("first_name", rs.getString("first_name"));
	            row.put("last_name", rs.getString("last_name"));
	            row.put("email", (rs.getString("email") != null) ? rs.getString("email") : "null");
	            row.put("address_id", rs.getInt("address_id"));
	            row.put("active", rs.getBoolean("active"));
	            row.put("create_date", rs.getString("create_date_formatted"));
	            row.put("last_update", rs.getString("last_update_formatted"));
	            resList.add(row);
	        }
		} catch(Exception e) {
			e.printStackTrace();
		}
		return resList;
	}

	public ArrayList<HashMap<String, Object>> getLanguages() {
        ArrayList<HashMap<String, Object>> resList = new ArrayList<HashMap<String, Object>>();
		try {
	        Statement stmt = null;
	        String query = "SELECT *, DATE_FORMAT(last_update, '%Y-%m-%dT%TZ') as last_update_formatted FROM language";
	        stmt = conx.createStatement();
	        ResultSet rs = stmt.executeQuery(query);
	        while (rs.next()) {
	            HashMap<String, Object> row = new HashMap<String, Object>();
	            row.put("language_id", rs.getInt("language_id"));
	            row.put("name", rs.getString("name"));
	            row.put("last_update", rs.getString("last_update_formatted"));
	            resList.add(row);
	        }
		} catch(Exception e) {
			e.printStackTrace();
		}
		return resList;
	}
	
	public ArrayList<HashMap<String, Object>> getFilms() {
        ArrayList<HashMap<String, Object>> resList = new ArrayList<HashMap<String, Object>>();
		try {
	        Statement stmt = null;
	        String query = "SELECT *, DATE_FORMAT(last_update, '%Y-%m-%dT%TZ') as last_update_formatted FROM film";
	        stmt = conx.createStatement();
	        ResultSet rs = stmt.executeQuery(query);
	        while (rs.next()) {
	            HashMap<String, Object> row = new HashMap<String, Object>();
	            row.put("film_id", rs.getInt("film_id"));
	            row.put("title", rs.getString("title"));
	            row.put("description", (rs.getString("description") != null) ? rs.getString("description") : "null");
	            row.put("release_year", (rs.getString("release_year") != null) ? rs.getString("release_year") : "null");
	            row.put("language_id", rs.getInt("language_id"));
	            row.put("original_language_id", rs.getInt("original_language_id"));
	            row.put("rental_duration", rs.getInt("rental_duration"));
	            row.put("rental_rate", rs.getFloat("rental_rate"));
	            row.put("length", (rs.getString("length") != null) ? rs.getString("length") : "null");
	            row.put("replacement_cost", rs.getFloat("replacement_cost"));
	            row.put("rating", rs.getString("rating"));
	            row.put("special_features", (rs.getString("special_features") != null) ? rs.getString("special_features") : "null");
	            row.put("last_update", rs.getString("last_update_formatted"));
	            resList.add(row);
	        }
		} catch(Exception e) {
			e.printStackTrace();
		}
		return resList;
	}

	public ArrayList<HashMap<String, Object>> getInventories() {
        ArrayList<HashMap<String, Object>> resList = new ArrayList<HashMap<String, Object>>();
		try {
	        Statement stmt = null;
	        String query = "SELECT *, DATE_FORMAT(last_update, '%Y-%m-%dT%TZ') as last_update_formatted FROM inventory";
	        stmt = conx.createStatement();
	        ResultSet rs = stmt.executeQuery(query);
	        while (rs.next()) {
	            HashMap<String, Object> row = new HashMap<String, Object>();
	            row.put("inventory_id", rs.getInt("inventory_id"));
	            row.put("film_id", rs.getInt("film_id"));
	            row.put("store_id", rs.getInt("store_id"));
	            row.put("last_update", rs.getString("last_update_formatted"));
	            resList.add(row);
	        }
		} catch(Exception e) {
			e.printStackTrace();
		}
		return resList;
	}

	public ArrayList<HashMap<String, Object>> getRentals() {
        ArrayList<HashMap<String, Object>> resList = new ArrayList<HashMap<String, Object>>();
		try {
	        Statement stmt = null;
	        String query = "SELECT *, DATE_FORMAT(return_date, '%Y-%m-%dT%TZ') as return_date_formatted, DATE_FORMAT(last_update, '%Y-%m-%dT%TZ') as last_update_formatted FROM rental";
	        stmt = conx.createStatement();
	        ResultSet rs = stmt.executeQuery(query);
	        while (rs.next()) {
	            HashMap<String, Object> row = new HashMap<String, Object>();
	            row.put("rental_id", rs.getInt("rental_id"));
	            row.put("rental_date", rs.getString("rental_date"));
	            row.put("inventory_id", rs.getInt("inventory_id"));
	            row.put("customer_id", rs.getInt("customer_id"));
	            row.put("staff_id", rs.getInt("staff_id"));
	            row.put("return_date", (rs.getString("return_date_formatted") != null) ? rs.getString("return_date_formatted") : "null");
	            row.put("last_update", rs.getString("last_update_formatted"));
	            resList.add(row);
	        }
		} catch(Exception e) {
			e.printStackTrace();
		}
		return resList;
	}

	public ArrayList<HashMap<String, Object>> getPayments() {
        ArrayList<HashMap<String, Object>> resList = new ArrayList<HashMap<String, Object>>();
		try {
	        Statement stmt = null;
	        String query = "SELECT *, DATE_FORMAT(payment_date, '%Y-%m-%dT%TZ') as payment_date_formatted, DATE_FORMAT(last_update, '%Y-%m-%dT%TZ') as last_update_formatted FROM payment";
	        stmt = conx.createStatement();
	        ResultSet rs = stmt.executeQuery(query);
	        while (rs.next()) {
	            HashMap<String, Object> row = new HashMap<String, Object>();
	            row.put("payment_id", rs.getInt("payment_id"));
	            row.put("customer_id", rs.getInt("customer_id"));
	            row.put("staff_id", rs.getInt("staff_id"));
	            //row.put("rental_id", (rs.getString("rental_id") != null) ? rs.getInt("rental_id") : -1);
	            row.put("rental_id", rs.getInt("rental_id"));
	            row.put("amount", rs.getFloat("amount"));
	            row.put("payment_date", rs.getString("payment_date_formatted"));
	            row.put("last_update", rs.getString("last_update_formatted"));
	            resList.add(row);
	        }
		} catch(Exception e) {
			e.printStackTrace();
		}
		return resList;
	}

	public ArrayList<HashMap<String, Object>> getCategories() {
        ArrayList<HashMap<String, Object>> resList = new ArrayList<HashMap<String, Object>>();
		try {
	        Statement stmt = null;
	        String query = "SELECT *, DATE_FORMAT(last_update, '%Y-%m-%dT%TZ') as last_update_formatted FROM category";
	        stmt = conx.createStatement();
	        ResultSet rs = stmt.executeQuery(query);
	        while (rs.next()) {
	            HashMap<String, Object> row = new HashMap<String, Object>();
	            row.put("category_id", rs.getInt("category_id"));
	            row.put("name", rs.getString("name"));
	            row.put("last_update", rs.getString("last_update_formatted"));
	            resList.add(row);
	        }
		} catch(Exception e) {
			e.printStackTrace();
		}
		return resList;
	}
	
	public ArrayList<HashMap<String, Object>> getFilmCategories() {
        ArrayList<HashMap<String, Object>> resList = new ArrayList<HashMap<String, Object>>();
		try {
	        Statement stmt = null;
	        String query = "SELECT *, DATE_FORMAT(last_update, '%Y-%m-%dT%TZ') as last_update_formatted FROM film_category";
	        stmt = conx.createStatement();
	        ResultSet rs = stmt.executeQuery(query);
	        while (rs.next()) {
	            HashMap<String, Object> row = new HashMap<String, Object>();
	            row.put("film_id", rs.getInt("film_id"));
	            row.put("category_id", rs.getInt("category_id"));
	            row.put("last_update", rs.getString("last_update_formatted"));
	            resList.add(row);
	        }
		} catch(Exception e) {
			e.printStackTrace();
		}
		return resList;
	}
	
	public ArrayList<HashMap<String, Object>> getActors() {
        ArrayList<HashMap<String, Object>> resList = new ArrayList<HashMap<String, Object>>();
		try {
	        Statement stmt = null;
	        String query = "SELECT *, DATE_FORMAT(last_update, '%Y-%m-%dT%TZ') as last_update_formatted FROM actor";
	        stmt = conx.createStatement();
	        ResultSet rs = stmt.executeQuery(query);
	        while (rs.next()) {
	            HashMap<String, Object> row = new HashMap<String, Object>();
	            row.put("actor_id", rs.getInt("actor_id"));
	            row.put("first_name", rs.getString("first_name"));
	            row.put("last_name", rs.getString("last_name"));
	            row.put("last_update", rs.getString("last_update_formatted"));
	            resList.add(row);
	        }
		} catch(Exception e) {
			e.printStackTrace();
		}
		return resList;
	}
	
	public ArrayList<HashMap<String, Object>> getFilmActors() {
        ArrayList<HashMap<String, Object>> resList = new ArrayList<HashMap<String, Object>>();
		try {
	        Statement stmt = null;
	        String query = "SELECT *, DATE_FORMAT(last_update, '%Y-%m-%dT%TZ') as last_update_formatted FROM film_actor";
	        stmt = conx.createStatement();
	        ResultSet rs = stmt.executeQuery(query);
	        while (rs.next()) {
	            HashMap<String, Object> row = new HashMap<String, Object>();
	            row.put("actor_id", rs.getInt("actor_id"));
	            row.put("film_id", rs.getInt("film_id"));
	            row.put("last_update", rs.getString("last_update_formatted"));
	            resList.add(row);
	        }
		} catch(Exception e) {
			e.printStackTrace();
		}
		return resList;
	}
	
	public ArrayList<HashMap<String, Object>> getFilmTexts() {
        ArrayList<HashMap<String, Object>> resList = new ArrayList<HashMap<String, Object>>();
		try {
	        Statement stmt = null;
	        String query = "SELECT * FROM film_text";
	        stmt = conx.createStatement();
	        ResultSet rs = stmt.executeQuery(query);
	        while (rs.next()) {
	            HashMap<String, Object> row = new HashMap<String, Object>();
	            row.put("film_id", rs.getInt("film_id"));
	            row.put("title", rs.getString("title"));
	            row.put("description", (rs.getString("description") != null) ? rs.getString("description") : "null");
	            resList.add(row);
	        }
		} catch(Exception e) {
			e.printStackTrace();
		}
		return resList;
	}
	
}
