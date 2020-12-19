package edu.cmu.cc.group.q3;

public class ROT13Util {

	private final static int NUM_LETTERS = 'z' - 'a' + 1;
	
	private final static char[] map = new char[NUM_LETTERS];;
	
	static {
		
		for (int i = 0; i < NUM_LETTERS; i++) {
			
			char curLetter = (char) ('a' + i);
			
			char inverseLetter;
			
			if (curLetter >= 'a' && curLetter <= 'm') {
				inverseLetter = (char) (curLetter + 13);
			} else {
				inverseLetter = (char) (curLetter - 13);
			}
			
			map[i] = inverseLetter;
		}
		
	}
	
	public static String decrypt(String input) {
		
		final int length = input.length();
		
		StringBuilder output = new StringBuilder(length);
		
		for (int i = 0; i < length; i++) {
			
			char curLetter = input.charAt(i);
			
			if (curLetter >= 'a' && curLetter <= 'z') {
				int letterIndex = curLetter - 'a';
				output.append(map[letterIndex]);
			} else {
				output.append(curLetter);
			}
		}
		
		return output.toString();
		
	}
	
}
