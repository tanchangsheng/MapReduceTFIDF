package aa.vectorisation;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class DocumentFrequencyMapReduce implements Mapper<List<Integer>, Integer, Integer>,
        Reducer<Integer, Integer, Integer, Integer> {


    @Override
    public Map<Integer, Integer> map(List<List<Integer>> words)
    {
        Map<Integer, Integer> map = new TreeMap<>();

        //Each unique word is a key

        //The value is the count of occurrences of the word
        for (List<Integer> row: words) {
            for (Integer i: row) {
                if (map.containsKey(i)) {
                    map.put(i, map.get(i) + 1);
                } else {
                    map.put(i, 1);
                }
            }
        }


        return map;
    }

    @Override
    public Map<Integer, Integer> reduce(Integer key, List<Integer> data)
    {
        TreeMap<Integer, Integer> map = new TreeMap<>();
        int totalCount = 0;
        for (Integer i: data)
        {
            //get data
            totalCount += i;
            //do something with value
            //output += value; //<-- sum?
        }

        map.put(key, totalCount);

        return map;
    }


}
