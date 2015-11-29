import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.commons.lang.StringUtils;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.*;

public class CountryRecognitionEngine { 
	
		private String[] locales;
		private String[] keywords;
		private String[] excludedCountries;
		private HashMap<String, String> athletesMap;
	
	    
	
	
		CountryRecognitionEngine(){}
		
		/**
		 * Initializes all the values
		 */
		public void startEngine(){
			populateLocales();
			populateKeywords();
			populateExcludedCountries();
			populateMap();
		}
		
		
		public void populateLocales(){
			this.locales = Locale.getISOCountries();
		}
		public void populateKeywords(){
			this.keywords = new String[]{"team", "go", "comeon", "cmon"}; 
		}
		/**
		 * I found certain player specific hashtags that became very popular during the specified period. I've added it
		 * to my calculation.
		 */
		public void populateMap(){
			this.athletesMap = new HashMap<String, String>();
			athletesMap.put("caster", "southafrica");
			athletesMap.put("phelps", "unitedstates");
			athletesMap.put("bolt", "jamaica");
			athletesMap.put("serena", "unitedstates");	
		}
		/**
		*Some countries are excluded from the shortnames filter to avoid anamolies. E.g. the short name for "turks and caicosis lands" is the same as the one for Turkey. Similarly, the short name for
		*Chile is Chi which is the same as the one for China. The general twitter trend suggests that china and turkey are far more popular on twitter than turks and caicosis lands and chile 			*respectively.
		**/
		public void populateExcludedCountries(){
			this.excludedCountries =  new String[]{"austria","niger","netherlandsantilles",
													"britishindianoceanterritory","mali", 
													"americansamoa","comoros", 
													"southgeorgiaandthesouthsandwichislands", 
													"turksandcaicosislands", "britishvirginislands", 
													"newcaledonia", "heardislandandmcdonaldislands", 
													"bonaire,sinteustatiusandsaba", "malta", 
													"malawi", "maldives", "chile", "anguilla", 
													"turkmenistan", "sanmarino", "grenada", 
													"greece", "greenland", "antiguaandbarbuda", 
													"eritrea", "monaco", "antarctica", "lesotho", 
													"montenegro", "mongolia", "montserrat", "angola", "timor-leste", "samoa", "marshallislands",
													"martinique", "gabon"};
		}
		
		/**
		 * This method takes the hashtag as input and makes the best possible guess
		 */
		public String getCountry(String  hashtag){
			
			for(String countryCode : locales){
				Locale obj = new Locale("", countryCode);
				String countryName = obj.getDisplayCountry();			
				countryName = removeSpacesandCommas(countryName);
				countryName = countryName.toLowerCase();
			
				if(containsCountryName(hashtag.toLowerCase(), countryName) || containsShortNames(hashtag.toLowerCase(), countryName) 
				  || containsNickNames(hashtag.toLowerCase(), countryName)) return countryName;

			}
			return (containsAthletesName(hashtag));
		}
		
		public boolean containsKeyWord(String hashtag){
			for(String keyword : keywords){
				if(hashtag.contains(keyword)) return true;
			}
			return false;
		}
	
	
		public boolean containsCountryName(String hashtag, String countryName){
			return (hashtag.contains(countryName));
		}
		
		public String containsAthletesName(String hashtag){
			for (Map.Entry<String, String> entry : athletesMap.entrySet()) {
			    String key = entry.getKey();
			    String value = entry.getValue();
			    if(hashtag.toLowerCase().contains(key)){
			    	return value;
			    }
			}
			return "SpamList";
		}
		/**
		 * I have also added a nicknames filter to match the country.
		 */
		public boolean containsNickNames(String hashtag, String countryName){
			String nickname = "";
			if("australia".equals(countryName)) nickname = "kangaroo";
			if("southafrica".equals(countryName)) nickname = "protea";
			if("newzealand".equals(countryName)) nickname = "kiwi";
			if("spain".equals(countryName)) nickname = "esp";
			if("netherlands".equals(countryName)) nickname = "dutch";
			if("germany".equals(countryName)) nickname = "deutschland";
			if("trinidadandtobago".equals(countryName)) nickname = "tandt";
			if("jamaica".equals(countryName)) nickname = "teamja";
			if("unitedkingdom".equals(countryName)) nickname = "gbr";
			if("japan".equals(countryName)) nickname = "jpn";
			if(nickname == "") return false;
			if(hashtag.contains(nickname)) return true;
			return false;
		}
		
		/**
		 * Checks whether the country has been excluded from shortnames filter.
		 */
		public boolean isAnExcludedCountry(String countryName){
			if(countryName.length() == 4) return true;
			if(countryName.substring(0,5).equals("saint")) return true;
			for(String excludedCountry : excludedCountries){
				if(countryName.equals(excludedCountry)) return true;
			}
			return false;
		}	
		/**
		 * I have added a short names filter that uses the first three letters of a country name to match the country.
		 */
		public boolean containsShortNames(String hashtag, String countryName){
			if(isAnExcludedCountry(countryName)){
				return false;		
			}
			String shortName = countryName.substring(0,3);
			if("unitedkingdom".equals(countryName)) shortName = "gb";
			if("unitedstates".equals(countryName)) shortName = "usa";
			if("unitedarabemirates".equals(countryName)) shortName = "uae";
			if("southafrica".equals(countryName)) shortName = "sa";
			if("andorra".equals(countryName)) shortName = "andor";
			if("thedemocraticrepublicofcongo".equals(countryName)) shortName = "congo";
			if("southkorea".equals(countryName)) shortName = "korea";
			if("northkorea".equals(countryName)) shortName = "korea";
			if("netherlands".equals(countryName)) shortName = "nl";
			if("indonesia".equals(countryName)) shortName = "ina";
			if("estonia".equals(countryName)) shortName = "teamest";
			return (hashtag.toLowerCase().contains(shortName));
		}
	
		/**
		 * removes spaces, dots and commas from a string.
		 */
		public String removeSpacesandCommas(String country){
			String refinedCountry = country.replaceAll(".", "");	
			refinedCountry = country.replaceAll(",", "");
			refinedCountry = country.replaceAll("\\s", "");
			return refinedCountry;
		}
	
	
	
	
}
