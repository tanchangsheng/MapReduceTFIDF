package aa.vectorisation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

@SuppressWarnings("Duplicates")
public class TfCallable implements Callable<List<List<Integer[]>>> {

    private List<List<Integer>> data;

    public TfCallable(List<List<Integer>> data){
        this.data = data;
    }

    @Override
    public List<List<Integer[]>> call(){

        List<List<Integer[]>> results = new ArrayList<>();

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
            List<Integer[]> rowTermFrequencies = new ArrayList<>();

            for(int key : wordToCountMap.keySet()){
                Integer[] wordToCountPair = new Integer[2];
                wordToCountPair[0] = key;
                wordToCountPair[1] = wordToCountMap.get(key);
                rowTermFrequencies.add(wordToCountPair);
            }

            results.add(rowTermFrequencies);
        }

        return results;
    }

}
