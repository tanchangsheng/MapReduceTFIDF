package aa.vectorisation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class MapReduceTfidfVectorizer {

    private static Map<String, Integer> vocab = new HashMap<>();

    //gets data from cleaned, combined file for positive and negative reviews
    private static String filename = System.getProperty("user.dir").substring(0,System.getProperty("user.dir").lastIndexOf('\\')) + "\\data\\cleanCombined.txt";

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
        if (!numericalFeatures.isEmpty()){
            //Call mapreduce
            //Create instance of the Mapper
            Mapper<List<Integer>, Integer, Integer> mapperCode = new DocumentFrequencyMapReduce();
            //Create instance of the Reducer
            Reducer<Integer, Integer, Integer, Integer> reducerCode = new DocumentFrequencyMapReduce();

            //How many shards?
            int numberOfShards = 4;

            //lots of output?
            boolean verbose = true;

            Map<Integer, Integer> resultMap = null;
            try {
                resultMap = MapReduce.mapReduce(mapperCode,
                        reducerCode,
                        numericalFeatures,
                        numberOfShards,
                        verbose);
            } catch (InterruptedException e)
            {
                e.printStackTrace();
            }

            for (Integer key : resultMap.keySet())
            {
                System.out.println(key + "=" + resultMap.get(key));
            }


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
