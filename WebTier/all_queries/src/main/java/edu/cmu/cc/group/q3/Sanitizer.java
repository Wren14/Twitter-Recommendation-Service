package edu.cmu.cc.group.q3;

public class Sanitizer {

	private final static String nonAlphaNumRegex = "((?<=[^a-zA-Z0-9])|(?=[^a-zA-Z0-9]))";
	
	public static String sanitizeText(String text) {
		//Split by non-alphanumeric characters, preserve delimiters
		String []words = text.split(nonAlphaNumRegex);
		
		//True if text contains words that needs to be censored
		//False if the text doesn't need to be censored
		boolean censored = false;
		
		for (int i = 0; i < words.length; i++) {
			
			if (!words[i].matches("\\W") &&  DriverQ3.bannedWords.contains(words[i].toLowerCase())) {
				words[i] = censorWord(words[i]);
				censored = true;
			}
			
		}
		
		if (!censored)
			return text;
		else {
			StringBuilder output = new StringBuilder(text.length());
			
			for (int i = 0; i < words.length; i++) {
				output.append(words[i]);
			}
			
			return output.toString();
		}
		
		
	}
	
	private static String censorWord(String word) {
		
		final int length = word.length();
		
		StringBuilder output = new StringBuilder(length);
		
		output.append(word.charAt(0));
		
		for (int i = 1; i < length - 1; i++) {
			output.append("*");
		}
		
		output.append(word.charAt(length - 1));
		
		
		return output.toString();
	}

	
}
