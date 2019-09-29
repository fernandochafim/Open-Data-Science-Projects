import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TextToolsDone {

	public static void main(String[] args) {
		final String ERROR_FORMAT = "[ERROR] You must write one word without numbers nor special characters!!";
		final String ERROR_MUST_BE_INTEGER = "[ERROR] You must write a integer number!!";
		
		Scanner in = new Scanner(System.in);;
		
		System.out.println("Write a word: ");
		String wordString = in.next().toLowerCase();
		
		Pattern pattern = Pattern.compile("[a-zA-Z]*");
		Matcher matcher = pattern.matcher(wordString);
	 
		if (!matcher.matches()) {
			System.out.println(ERROR_FORMAT);
			System.out.println("Program finished!!");;
		}else {
			char[] word = wordString.toCharArray();
			int beginning, ending, numShifts;
			char letter;
			try {
				System.out.println("Write beginning index (starts '0'): ");
					beginning = in.nextInt();
				System.out.println("Write ending index: ");
					ending = in.nextInt();
			
		    	System.out.println(extractString(word,beginning,ending));
		    	
		    	System.out.println("\nEnter a letter: ");
		    		letter = in.next().toLowerCase().charAt(0);
				
		    	System.out.println(removeChar(word,letter));
		    	
		    	
		    	System.out.println("\nEnter positions to shift: ");
		    		numShifts = in.nextInt();
		    		
		    	System.out.println(shiftWord(word,numShifts));
		    	
		    	System.out.println("Bye!");
			}catch(Exception e) {
				System.out.println(ERROR_MUST_BE_INTEGER+e.getMessage());
			}				    	
		}
		
		in.close();
	}

	static char[] extractString(char[] word_temp, int beginning, int ending) {
		final String ERROR_BEGINNING_NEGATIVE = "[ERROR] The beginning index is smaller than 0!!";
		final String ERROR_BEGINNING_HIGHER_ENDING = "[ERROR] The beginning index is higher than the ending index!!";
		final String ERROR_ENDING_HIGHER_LENGTH = "[ERROR] The ending index is greater than the length of the word!!";
		
		if(beginning<0){
			System.err.println(ERROR_BEGINNING_NEGATIVE);
		} else if (beginning > ending){
			System.err.println(ERROR_BEGINNING_HIGHER_ENDING);
		} else if (ending > word_temp.length) {
			System.err.println(ERROR_ENDING_HIGHER_LENGTH);
		}
		
		int j, count = 0, n = word_temp.length;
                for (int i = j = 0; i < n; i++) { 
                    if ((i >= beginning) && (i <= ending)) 
                        word_temp[j++] = word_temp[i]; 
                    else
                        count++; 
                } 
                while(count > 0) 
                { 
                    word_temp[j++] = '\0'; 
                    count--; 
                }
        return word_temp;
	}
	
	static char[] removeChar(char[] word_temp2, char letter) {
        int j, count = 0, n = word_temp2.length;
		for (int i = j = 0; i < n; i++) 
        { 
        if (word_temp2[i] != letter) 
        word_temp2[j++] = word_temp2[i]; 
        else
            count++; 
        } 
        while(count > 0) 
        { 
        word_temp2[j++] = '\0'; 
        count--; 
        }
        return word_temp2;
	}
	
	static char[] shiftWord(char[] word_temp3, int numShifts) {
		char temp_a = word_temp3[0];
		char temp_b = word_temp3[numShifts];
		
		word_temp3[0] = temp_b;
		word_temp3[numShifts] = temp_a;		
		return word_temp3;
	}
}