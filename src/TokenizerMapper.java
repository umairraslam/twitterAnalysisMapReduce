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

public class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> { 
	    private final IntWritable one = new IntWritable(1);
	    private Text data;
	    private String hashtag;	
	    
	    TokenizerMapper(){
	      this.data = new Text();
	    }
	    
	    void setData(Text data){
	      this.data = data;
	    }
	    
	    Text getData(){
	      return this.data;
	    }
	    
	    IntWritable getOne(){
	      return this.one;
	    }
	    
	 
	    
	    String getHashtag(){
	      return this.hashtag;
	    }
	    
	 
		  /**
		   * This method checks if the tweet contains hashtags or not by analyzing the hashtags string array.
		   */
		  public boolean areHashtagsPresent(String[] hashtags){
			return (hashtags.length > 1 && hashtags[0] != "");	
		  }
		
		
	
		  /**
		   * Extracts the hashtag from the tweet entry.
		   */
		public void setHashtag(String entry){
			int startIndex = 0; // declaration and initialization of integers that store
			int endIndex = 0;   // start and end indices
			if(StringUtils.ordinalIndexOf(entry, ";" ,4) > -1){ 
				startIndex = StringUtils.ordinalIndexOf(entry, ";", 2) + 1;//index of second occurence of ;
				endIndex = StringUtils.ordinalIndexOf(entry, ";", 3); // index of third occurence of ;
				this.hashtag = entry.substring(startIndex, endIndex); //hashtag extracted using the substring method of class String
			}	
		}	
		
	public String getCountryWithMaxOccurences(List<String> list){
		int current, maximum = 0; String country = "SpamList";
		for(String value : list){
			current = Collections.frequency(list, value);
			if(current > maximum){
				maximum = current;
				country = value;			
			}
		}
		return country;	
	}
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
			String dump = value.toString(); //value of type Text converted to String
			List<String> countries = new ArrayList<String>();
			
			this.setHashtag(dump); //hashtag is present between the second and third occurence of the semi-colon
			
		
			int counter = 0; //this counter counts the number of hashtags that contain atleast one keyword 
			List<Integer> listOfIndexes = new ArrayList<Integer>(); // this list stores the indices of those hashtags that contain atleast one keyword
			
			String[] hashtags = this.getHashtag().split(" "); //hashtags are separated by a space
			if(areHashtagsPresent(hashtags)){
				CountryRecognitionEngine cre = new CountryRecognitionEngine(); //Country Recognition Engine declaration and initialization
				cre.startEngine(); //Country Recognition Engine started
				for(int i=0; i<hashtags.length; i++){ 
					if(cre.containsKeyWord(hashtags[i].toLowerCase())){ //if hashtags contains atleast one of the keywords, add the index to the list of indices
						listOfIndexes.add(i);	
						counter++;		
					}		
				}
				/**
				 * We ignore those tweets that contain multiple non-unique hashtags. To exclude spam, we ignore tweets with hashtags that support multiple teams. To esnure that, we
				   count the occurences of first hashtag of a tweet.If the number of occurences of the first hashtag is not equal to the 
				 * total number of hashtags, we add the tweet to "IgnoreList". If the tweet has no hashtags, we add it to the "NoHashtags" list. 
				 */
				if(listOfIndexes.size() > 0){
						for(int index : listOfIndexes){
							if(hashtags.length > listOfIndexes.get(0))
								countries.add(cre.getCountry(hashtags[index]));
						}
						String country = getCountryWithMaxOccurences(countries);
						data.set(country);
						context.write(data, new IntWritable(listOfIndexes.size()));
				}
			
			}else{
				data.set("NoHashtags");
				context.write(data, one);
			}	
	
	
	    }
	}
