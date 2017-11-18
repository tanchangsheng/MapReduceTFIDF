package aa.vectorisation;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;


/**
 * @author kevinsteppe
 * based on ideas from Google (http://labs.google.com/papers/mapreduce.html)
 * and the implementation from http://semanticlinguine.blogspot.com
 *
 * The intention is to create a structured teaching framework to emphasize coding in a MapReduce parralel style for
 * java-taught students.  It focuses on structuring the problem into an explicit Map step (producing key-value pairs
 * as in the google version) followed by a Reduce step (intaking a map of key-List(value) objects and outputing
 * another map of key-value pairs.
 *
 * This is a 'poor man' implementation - nothing distributed, no fault tolerance, no straggler recovery.
 *
 * 26/4/17 - Generics.
 * This upgrade from the original to include generics for type checking.  There are five types for MapReduce:
 * 1) Incoming data type "T"
 * 2) Mapper produced Keys type "MK"
 * 3) Mapper produced Values type "MV"
 * 4) Reducer produced Keys type "RK"
 * 5) Reducer produced Values type "RV"
 *
 * Mapreduce.mapreduce is typed <T, MK, MV, RK, RV>
 *     The ouptut of mapreduce is Map<RK, List<RV>>
 *
 * Mapper is typed <T,MK,MV> - produces Map<MK,MV> from data List<T>
 * Reducer is typed <RK,RV,MK,MV> - produces Map<RK,RV> from MK, List<MV>
 *
 * 26/4/17 - Callables.
 * This upgrade wraps the Mapper and Reducer tasks in Callable objects, which are submitted to
 * a ForkJoinPool.
 *
 * The collection steps then loop through each Future, checks if done, then merges into the collated Map.  This means
 * that data merge might be concurrent with Mapper/Reducer tasks if there are stragglers.
 *
 */
public class MapReduce
{
    protected static boolean _verbose;

    /**
     *
     * @param mapper instance to provide map function.  Because only one instance is used: 1) don't synchronize it, 2) it should have no non-local effects
     * @param reducer instance to provide reduce funtion.  Same warnings
     * @param data a list of data.  This could also be a list of *references* to the data location, such as a list of filenames.
     * @param shards approximate number of shards to split the data into.
     *          A shard size is generated, and the list broken into shards of that size:
     *              shard Size ~ ceiling(data.size / shards)
     *              the last shard being the remainder, ie: data.size % shard Size
     *          for example: if data.size = 30 and shards = 3, then 3 shards of 10 items are generated
     *                       if data.size = 30 and shards = 4, then 3 shards of 8 items and 1 of 6 items
     *                       if data.size = 30 and shards = 13, then 10 shards of 3 items are generated
     *          Thus for some shard sizes, the actual number of shards may be smaller then specified.  It will not be more.
     *          One mapper is launched for each shard generated.
     *
     *          One reducer is launched for every key generated by the mappers
     *
     *
     * @param verbose true will make the framework output (S.O.P) a bunch of text about what it is doing.
     *
     * @return a HashMap with keys from the reducer, and a list of all outputs for that key (across all reducers), if your
     *          reducers only use the key given to them then the list will always be length 0.
     *
     * Generics note:
     *
     * This takes in list of data of type T.
     * Uses Mapper<T,MK,MV> to produce Map<MK,MV> from List<T>.
     * Uses Reducer<MK,MV,RK,RV> to produce Map<RK,RV> from Key MK and List of values MV
     */
    public static <T, MK, MV, RK, RV> Map<RK, RV> mapReduce(Mapper<T, MK, MV> mapper,
                                                                  Reducer<MK, MV, RK, RV> reducer,
                                                                  List<T> data, int shards, boolean verbose) throws InterruptedException
    {
        _verbose = verbose;

        //creates mapper tasks, allocates each a shard
        List<Base<T,MK,MV>> mapperTasks = createMappers(data, shards, mapper);

        //creates a thread for each task, starts the thread, waits for all threads to finish
        List<Future<Map<MK,MV>>> mapperResults = runThreads(mapperTasks);

        //produces a merged hashmap from the mapper results.  mapper values with the same key are added to a list
        Map<MK, List<MV>> intermediates = getResults(mapperResults);

        //create reducer tasks.  takes the intermediates as input.
        List<Base<MV,RK,RV>> reducerTasks = createReducers(intermediates, reducer);

        //creates a thread for each task, starts the thread, waits for all threads to finish
        List<Future<Map<RK,RV>>> reducerFutures = runThreads(reducerTasks);

        //produces a merged hashmap from the reducer results.  reducer values with the same key are added to a list
        Map<RK, List<RV>> reducerResults = getResults(reducerFutures);

        Map<RK, RV> finalResults = clearValueList(reducerResults);

        return finalResults;
    }


    private static <T,MK,MV> List<Base<T,MK,MV>> createMappers(List<T> data, int shards, Mapper<T,MK,MV> mapper)
    {
        List<Base<T,MK,MV>> mappers = new ArrayList<>(shards);
        int shardSize = (int) Math.ceil(data.size() / (float)shards);

//        if (_verbose) System.out.println("Sharding data -> " + data.size() / shardSize + " shards in the input to send to mappers");

        for (int pointer = 0; pointer < data.size(); pointer+= shardSize)
        {
            int end = Math.min(pointer + shardSize, data.size());
            List<T> dataSubList = data.subList(pointer, end);
            mappers.add(new MapperBase<>(mapper, dataSubList));

            if (_verbose) System.out.println((end - pointer) + " values sent to mapper -> " + dataSubList);
        }
        return mappers;
    }

    private static <MK,MV,RK,RV> List<Base<MV, RK,RV>> createReducers(Map<MK,List<MV>> intermediates, Reducer<MK,MV,RK,RV> reducer)
    {
        List<Base<MV,RK,RV>> reducers = new ArrayList<>(intermediates.size());
        for (MK key : intermediates.keySet())
        {
            List<MV> dataList = intermediates.get(key);
            reducers.add(new ReducerBase<>(reducer, key, dataList));

            if (_verbose) System.out.println(dataList.size() + " values sent to reducer ["+key+"] -> " + dataList);
        }
        return reducers;
    }

    private static ForkJoinPool pool = new ForkJoinPool();

    private static <T,K,V> List<Future<Map<K, V>>> runThreads(List<Base<T,K,V>> tasks) throws InterruptedException
    {
        List<Future<Map<K,V>>> futureList = new ArrayList<>(tasks.size());
        for (Base<T,K,V> task : tasks)
        {
            futureList.add( pool.submit(task) );
        }

        return futureList;
    }

    private static <K,V> Map<K,List<V>> getResults(List<Future<Map<K,V>>> futureList)
    {
        Map<K, List<V>> resultMap = new HashMap<>(futureList.size() / 3);

        while (futureList.size() > 0)
        {
            Iterator<Future<Map<K, V>>> futureIterator = futureList.iterator();
            while (futureIterator.hasNext())
            {
                Future<Map<K, V>> f = futureIterator.next();

                try
                {
                    if (f.isDone())
                    {
                        Map<K, V> results = f.get();
                        Set<K> keySet = results.keySet();

                        for (K key : keySet)
                        {
                            V value = results.get(key);
                            List<V> list = resultMap.get(key);
                            if (list == null)
                            {
                                list = new ArrayList<>();
                                resultMap.put(key, list);
                            }
                            list.add(value);
                        }
                        futureIterator.remove();
                    }
                    Thread.yield();
                } catch (InterruptedException | ExecutionException e)
                {
                    e.printStackTrace();
                }
            }
        }

        return resultMap;
    }

    private static <RK, RV> Map<RK, RV> clearValueList(Map<RK, List<RV>> map)
    {
        return
                map.entrySet().stream()
                        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().get(0)));
    }

    private abstract static class Base<T,K,V> implements Callable<Map<K,V>>
    {
        List<T> dataList;
    }

    private static class MapperBase<T,MK,MV> extends Base<T,MK,MV> implements Callable<Map<MK,MV>>
    {
        Mapper<T,MK,MV> mapper;

        private MapperBase(Mapper<T,MK,MV> mapper, List<T> data)
        { this.mapper = mapper; this.dataList = data; }

        @Override
        public Map<MK,MV> call()
        { return mapper.map(dataList); }
    }

    private static class ReducerBase<MK, MV, RK, RV> extends Base<MV,RK,RV> implements Callable<Map<RK,RV>>
    {
        Reducer<MK, MV, RK, RV> reducer;
        MK key;

        private ReducerBase(Reducer<MK, MV, RK, RV> reducer, MK key, List<MV> data)
        { this.reducer = reducer; this.key = key; this.dataList = data; }

        @Override
        public Map<RK,RV> call()
        {
            return reducer.reduce(key, dataList);
        }
    }

}
