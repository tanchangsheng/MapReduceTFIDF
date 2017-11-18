package aa.vectorisation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class MapReduceTfidfVectorizer {

    private static Map<String, Integer> vocab = new HashMap<>();

    //gets data from cleaned, combined file for positive and negative reviews
    private static String filename = System.getProperty("user.dir") + "\\src\\aa\\data\\cleanCombined.txt";

    public static void main(String[] args) {

        // creating a numerical index of all the words in file
        createVocab(filename);

        // converting all words in review to numbers
        List<List<Integer>> numericalFeatures = new ArrayList<>();

        try {

             numericalFeatures = Files.lines(Paths.get(filename))
                    .map(x -> x.split(","))
                    .map(x -> x[2])
                    .map(x -> reverseIndexing(x))
                    .collect(Collectors.toList());

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public static void createVocab(String filename){

        List<String> words = new ArrayList<>();

        try {
             words = Files.lines(Paths.get(filename))
                    .flatMap(x -> {
                        String[] parts = x.split(",");
                        String[] reviewWords = parts[2].split(" ");
                        return Arrays.stream(reviewWords);
                    })
                     .limit(10)
                     .distinct()
                     .collect(Collectors.toList());

        } catch (IOException e) {
            e.printStackTrace();
        }

        for(String word : words){
            int id = 0;
            vocab.put(word, id);
            id++;
        }
    }

    public static List<Integer> reverseIndexing(String words){

        List<Integer> numericalWords = new ArrayList<>();

        numericalWords = Arrays.stream(words.split(" "))
                .map(x -> vocab.get(x))
                .collect(Collectors.toList());

        return numericalWords;
    }

}
