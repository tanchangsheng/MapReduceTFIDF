package aa.vectorisation;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


@SuppressWarnings("Duplicates")
public class SerialTfidfVectorizor {

//    private static String filename = "/Users/changsheng/OneDrive - Singapore Management " +
//            "University/Work/Current Mods/IS303 AA/week 6/Week6_PP (Callables and Futures)/src/450kfood.txt";
    private static Map<String, Integer> vocab = new HashMap<>();
    private static String filename = "../data/cleanLargeCombined.txt";
    private static List<List<Integer>> numericalFeatures = new ArrayList<>();
    private static List<List<Double[]>> tfidfVectors = new ArrayList<>();
    private static Map<Integer, Integer> vocabDocumentFrequency = new HashMap<>();

    public static void main(String[] args) {

        createVocab(filename);

        // converting all words in review to numbers
        try {

//             amazon product review
            numericalFeatures = Files.lines(Paths.get(filename))
                    .map(x -> x.split(","))
                    .map(x -> x[2])
                    .map(x -> reverseIndexing(x))
                    .collect(Collectors.toList());

//            // foods.txt
//            numericalFeatures = Files.lines(Paths.get(filename))
//                    .map(x -> reverseIndexing(x))
//                    .collect(Collectors.toList());

        } catch (IOException e) {
            e.printStackTrace();
        }

        if (numericalFeatures.isEmpty()) {
            // stop execution of program
            System.exit(1);
        }

        long startTime = System.currentTimeMillis();

        getDocFrequency();

        getTfidfVector(true);

        long endTime = System.currentTimeMillis();

        System.out.printf("Serial Tfidf vectorisation completed in %d ms", (endTime - startTime));

    }

    /**
     * @param doc  list of wordIDs in a document
     * @param term wordID representing a term
     * @return term frequency of term in document
     */
    public static int tf(List<Integer> doc, int term) {
        int result = 0;
        for (int i : doc) {
            if (term == i)
                result++;
        }
        return result;
    }

    /**
     * @param doc  list of wordIDs in a document
     * @param term wordID representing a term
     * @return term frequency of term in document
     */
    public static double tfIdf(int term, List<Integer> doc) {

        int tf = tf(doc, term);

        double idf = Math.log(vocabDocumentFrequency.get(term) * 1.0 / tf);

        return  tf * idf;

    }



    public static void getDocFrequency(){

        for(List<Integer> row : numericalFeatures){

            for(int term : row){

                if(vocabDocumentFrequency.containsKey(term)){

                    int existingCount = vocabDocumentFrequency.get(term);
                    existingCount ++;
                    vocabDocumentFrequency.put(term, existingCount);

                }else{

                    vocabDocumentFrequency.put(term, 1);

                }
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
//                        String[] reviewWords = x.split(" ");
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

    public static void getTfidfVector(boolean verbose){

        for(List<Integer> doc : numericalFeatures){

            List<Double[]> docTfidf = new ArrayList<>();

            for(int term : doc){
                Double[] pair = new Double[2];
                pair[0] = Double.valueOf(term);
                pair[1] = tfIdf(term, doc);
                docTfidf.add(pair);
            }

            tfidfVectors.add(docTfidf);

        }

        if (verbose){
            for(List<Double[]> tfVector : tfidfVectors){

                System.out.print("[");
                for(Double[] pair : tfVector){
                    System.out.print(Arrays.toString(pair));
                }
                System.out.println("]");
            }
        }

    }





}
