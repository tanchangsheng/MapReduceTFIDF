package aa.processing;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class ProcessData {

    private static String stopwordsFile = "../data/stopwords";
    private static String inputDocument = "../data/largePositive.txt";
    private static String outputDocument = "../data/cleanedLargePositive.txt";
    private static String label = "+";

    private static List<String> stopwords;

    public static void main(String[] args) {

        loadStopWords();

        // clean and transform amazon data
        processFood();

//        clean and transdorm amazon product review
//        transformData();

    }

    public static void processFood(){

        String inputDocument = "/Users/changsheng/OneDrive - Singapore Management " +
                "University/Work/Current Mods/IS303 AA/week 6/Week6_PP (Callables and Futures)/src/foods.txt";

        String outputDocument = "/Users/changsheng/OneDrive - Singapore Management " +
                "University/Work/Current Mods/IS303 AA/week 6/Week6_PP (Callables and Futures)/src/450kfood.txt";

        List<String> data = new ArrayList<>();

        try {
            data = Files.lines(Paths.get(inputDocument), Charset.forName("ISO-8859-1"))
                    .filter(x -> x.startsWith("review/text:"))
                    .limit(450000)
                    .map(x -> x.replaceFirst("review/text:", ""))
                    .map(x -> clean(x))
                    .collect(Collectors.toList());


        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(data.size());

        File outputFile = new File(outputDocument);

        try ( FileWriter fr = new FileWriter(outputFile)){

            for (String line : data){
                fr.write(line + "\n");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }





    }

    // to transform markup language to csv of DocId,Label,Review,
    // book1, +, this book is amazing
    public static void transformData(){
        List<String> data = new ArrayList<>();

        File file = new File(inputDocument);

        try (FileReader fileReader = new FileReader(file)){

            BufferedReader bufferedReader = new BufferedReader(fileReader);
            StringBuffer stringBuffer = new StringBuffer();

            String line;
            boolean isID = false;
            boolean isReview = false;

            while ((line = bufferedReader.readLine()) != null) {

                line = line.trim(); // remove empty spaces

                if (line.equalsIgnoreCase("<unique_id>")) {
                    // indicate start of ID in coming rows
                    isID = true;
                    continue;
                } else if (line.equalsIgnoreCase("</unique_id>")){
                    // indicate end of ID
                    isID = false;
                }

                if(isID){
                    line = line.replaceAll(",", "");
                    stringBuffer.append(line);
                }

                if (line.equalsIgnoreCase("<review_text>")){
                    // indicate start of review in coming rows
                    isReview = true;
                    continue;
                } else if (line.equalsIgnoreCase("</review_text>")){
                    // indicate end of review
                    isReview = false;
                    // end string buffer and add string to list
                    data.add(stringBuffer.toString());
                    stringBuffer.setLength(0);
                }
                if(isReview){
                    String cleaned = clean(line);
                    stringBuffer.append("," + label + "," + cleaned);
                }



            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(data.size());
        File outputFile = new File(outputDocument);

        try ( FileWriter fr = new FileWriter(outputFile)){

            for (String line : data){
                fr.write(line + "\n");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // remove stop words, punctuations. Using streams
    public static String clean(String rowOfText){

        List<String> cleanWords = Arrays.stream(rowOfText.split(" "))
                .filter(x -> x.matches("^[a-zA-Z0-9]*$")) //retrieve only alphanumeric
                .map(x -> x.replaceAll(",",""))
                .map(String::toLowerCase) //convert all to lower case
                .filter(x -> !stopwords.contains(x)) // remove stop words
                .collect(Collectors.toList());

        return String.join(" ", cleanWords);

    }

    /*
     *  load stop words into list
     */
    public static void loadStopWords(){

        try {

            stopwords = Files.lines(Paths.get(stopwordsFile))
                    .collect(Collectors.toList());

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
