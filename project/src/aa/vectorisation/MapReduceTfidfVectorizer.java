package aa.vectorisation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.stream.Collectors;


@SuppressWarnings("Duplicates")
public class MapReduceTfidfVectorizer {

    //gets data from cleaned, combined file for positive and negative reviews
//    private static String filename = System.getProperty("user.dir").substring(0,System.getProperty("user.dir").lastIndexOf('\\')) + "\\data\\cleanCombined.txt";

//    private static String filename = "/Users/changsheng/OneDrive - Singapore Management " +
//            "University/Work/Current Mods/IS303 AA/week 6/Week6_PP (Callables and Futures)/src/50kfood.txt";
    private static String filename = "../data/cleanLargeCombined.txt";
    private static int numOfThreads = 8;
    private static Map<String, Integer> vocab = new HashMap<>();
    private static List<List<Integer>> numericalFeatures = new ArrayList<>();
    private static Map<Integer, Integer> vocabDocumentFrequency = new HashMap<>();
    private static List<List<Integer[]>> tfVectors = new ArrayList<>();
    private static List<List<Double[]>> tfidfVectors = new ArrayList<>();

    public static void main(String[] args) {

        // creating a numerical index of all the words in file

        createFoodVocab(filename);

        // converting all words in review to numbers
        try {

            numericalFeatures = Files.lines(Paths.get(filename))
                    .map(x -> x.split(","))
                    .map(x -> x[2])
                    .map(x -> reverseIndexing(x))
                    .collect(Collectors.toList());

        } catch (IOException e) {
            e.printStackTrace();
        }

        if (numericalFeatures.isEmpty()) {
            // stop execution of program
            System.exit(1);
        }

        long startTime = System.currentTimeMillis();

        //Call mapreduce to obtain document frequency for each word
        //lots of output?
        boolean verbose = false;
        // How many shard?
        int numberOfShards = 8;
        getDocumentFrequency(numericalFeatures, numberOfShards,verbose);

        getTfidfVector(numOfThreads,false, vocabDocumentFrequency);

        long endTime = System.currentTimeMillis();

        System.out.printf("Map reduce Tfidf vectorisation completed in %d ms", (endTime - startTime));


    }

    public static void createVocab(String filename){

        List<String> words = new ArrayList<>();

        try {
            words = Files.lines(Paths.get(filename))
                    .flatMap(x -> {
                        String[] parts = x.split(",");
//                        System.out.println(Arrays.toString(parts));
                        String[] reviewWords = parts[2].split(" ");
                        return Arrays.stream(reviewWords);
                    })
                    .distinct()
                    .collect(Collectors.toList());

        } catch (IOException e) {
            e.printStackTrace();
        }

        int id = 0;
        for(String word : words){
            vocab.put(word, id);
            id++;
        }
        System.out.println("Number of unique tokens : " + vocab.keySet().size());
    }

    public static void createFoodVocab(String filename){

        List<String> words = new ArrayList<>();

        try {
            words = Files.lines(Paths.get(filename))
                    .flatMap(x -> {
                        String[] reviewWords = x.split(" ");
                        return Arrays.stream(reviewWords);
                    })
                    .distinct()
                    .collect(Collectors.toList());

        } catch (IOException e) {
            e.printStackTrace();
        }

        int id = 0;
        for(String word : words){
            vocab.put(word, id);
            id++;
        }
        System.out.println("Number of unique tokens : " + vocab.keySet().size());
    }

    public static List<Integer> reverseIndexing(String words){

        List<Integer> numericalWords = new ArrayList<>();

        numericalWords = Arrays.stream(words.split(" "))
                .map(x -> vocab.get(x))
                .collect(Collectors.toList());

        return numericalWords;
    }

    public static void getDocumentFrequency(List<List<Integer>> numericalFeatures, int numberOfShards, boolean verbose){

        //Create instance of the Mapper
        Mapper<List<Integer>, Integer, Integer> mapperCode = new DocumentFrequencyMapReduce();
        //Create instance of the Reducer
        Reducer<Integer, Integer, Integer, Integer> reducerCode = new DocumentFrequencyMapReduce();

        try {
            vocabDocumentFrequency = MapReduce.mapReduce(mapperCode,
                    reducerCode,
                    numericalFeatures,
                    numberOfShards,
                    verbose);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }

        if(verbose){
            for (Integer key : vocabDocumentFrequency.keySet())
            {
                System.out.println(key + "=" + vocabDocumentFrequency.get(key));
            }
        }


    }

    public static void getTfidfVector(int numOfThreads, boolean verbose, Map<Integer, Integer> vocabDocumentFrequency){

        int dataSize = numericalFeatures.size();
        int sizeOfEachSet = dataSize / numOfThreads;
        int remainder = dataSize % numOfThreads;


        List<TfidfCallable> tfidfCallables = new ArrayList<>();

        ForkJoinPool pool = new ForkJoinPool();

        for (int i = 0; i < numOfThreads; i++) {
            List<List<Integer>> subList;

            // include the remainder rows of reviews due to division by thread size
            if (i + 1 == numOfThreads && remainder > 0) {
                subList = numericalFeatures.subList(i * sizeOfEachSet, dataSize);
            } else {
                subList = numericalFeatures.subList(i * sizeOfEachSet, (i + 1) * sizeOfEachSet);
            }
            tfidfCallables.add(new TfidfCallable(subList, vocabDocumentFrequency));
        }

        List<Future<List<List<Double[]>>>> tfFutures = pool.invokeAll(tfidfCallables);

        for (Future<List<List<Double[]>>> x : tfFutures) {

            try {

                List<List<Double[]>> subList = x.get();
                tfidfVectors.addAll(subList);

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

        }

        if(verbose){
            for(List<Double[]> tfVector : tfidfVectors){

                System.out.print("[");
                for(Double[] pair : tfVector){
                    System.out.print(Arrays.toString(pair));
                }
                System.out.println("]");
            }
        }


    }

    public static void getTfVector(int numOfThreads, boolean verbose){

        int dataSize = numericalFeatures.size();
        int sizeOfEachSet = dataSize / numOfThreads;
        int remainder = dataSize % numOfThreads;


        List<TfCallable> tfCallables = new ArrayList<>();

        ForkJoinPool pool = new ForkJoinPool();

        for (int i = 0; i < numOfThreads; i++) {
            List<List<Integer>> subList;

            // include the remainder rows of reviews due to division by thread size
            if (i + 1 == numOfThreads && remainder > 0) {
                subList = numericalFeatures.subList(i * sizeOfEachSet, dataSize);
            } else {
                subList = numericalFeatures.subList(i * sizeOfEachSet, (i + 1) * sizeOfEachSet);
            }
            tfCallables.add(new TfCallable(subList));
        }

        List<Future<List<List<Integer[]>>>> tfFutures = pool.invokeAll(tfCallables);

        for (Future<List<List<Integer[]>>> x : tfFutures) {

            try {

                List<List<Integer[]>> subList = x.get();
                tfVectors.addAll(subList);

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

        }

        if(verbose){
            for(List<Integer[]> tfVector : tfVectors){

                System.out.print("[");
                for(Integer[] pair : tfVector){
                    System.out.print(Arrays.toString(pair));
                }
                System.out.println("]");
            }
        }

    }

}
