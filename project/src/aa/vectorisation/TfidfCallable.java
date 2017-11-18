package aa.vectorisation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@SuppressWarnings("Duplicates")
public class TfidfCallable implements Callable<List<List<Double[]>>> {

    private List<List<Integer>> data;
    private Map<Integer, Integer> docFrequencyMap;

    public TfidfCallable(List<List<Integer>> data, Map<Integer, Integer> docFrequencyMap){
        this.data = data;
        this.docFrequencyMap = docFrequencyMap;
    }

    @Override
    public List<List<Double[]>> call(){

        List<List<Double[]>> results = new ArrayList<>();

        for(List<Integer> row : data){

            // map of wordID to it's count
            HashMap<Integer, Integer> wordToCountMap = new HashMap<>();

            for(int i : row){
                if(wordToCountMap.containsKey(i)){
                    int existingCount = wordToCountMap.get(i);
                    existingCount++;
                    wordToCountMap.put(i, existingCount);
                }else{
                    wordToCountMap.put(i, 1);
                }
            }

            // Convert map to list of [wordID,count] pairs
            List<Double[]> rowTermFrequencies = new ArrayList<>();

            for(int key : wordToCountMap.keySet()){
                Double[] wordToCountPair = new Double[2];
                wordToCountPair[0] = Double.valueOf(key);
                int tf = wordToCountMap.get(key);
                double tfidf = calculateTfidf(key,tf);
                wordToCountPair[1] = tfidf;
                rowTermFrequencies.add(wordToCountPair);
            }

            results.add(rowTermFrequencies);
        }

        return results;
    }

    public double calculateTfidf(int wordID, int tf){

        int docFrequency = docFrequencyMap.get(wordID);

        double idf = Math.log(docFrequency/tf);

        double tfidf = tf * idf;

        return tfidf;
    }

}
