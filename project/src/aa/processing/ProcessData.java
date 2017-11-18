package aa.processing;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ProcessData {

    private static String stopwordsFile = "../../data/stopwords";
    private static String inputDocument = "../../data/allNegative.txt";
    private static String outputDocument = "../../data/cleanedAllNegative.txt";

    private static List<String> stopwords;

    public static void main(String[] args) {

        loadStopWords();

        transformData();

    }

    // to transform markup language to csv of DocId, Label, Review,
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
            int tagCount = 0;

            while ((line = bufferedReader.readLine()) != null) {

                line = line.trim(); // remove empty spaces

                if (line.equalsIgnoreCase("<unique_id>")) {
                    // indicate start of ID in coming rows
                    if(tagCount == 0){
                        isID = true;
                        tagCount += 1;
                    } else if(tagCount == 1){
                        tagCount = 0;
                    }
                    continue;
                } else if (line.equalsIgnoreCase("</unique_id>")){
                    // indicate end of ID
                    isID = false;
                }

                if(isID){
                    line.replaceAll(",", "");
                    stringBuffer.append(line + ",");
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
                    stringBuffer.append(cleaned);
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
