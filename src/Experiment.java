package uk.ac.ncl;



import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class Experiment {
    public static String M;
    public static String D;

    public static void main(String[] args) {
        // List of dataset names
        List<String> datasets = Arrays.asList("WN18RR-LV");
        // List of methods
        List<String> methods = Arrays.asList("TransE", "PTransE");

        for (String dataset : datasets) {
            for (String method : methods) {
                D=dataset;
                M=method;
                String[] args1 = {"-c", "data/" + dataset + "/config.json", "-r"};

                // Run the experiment with the current dataset and method
                Run.main(args1);

                // Read the last 7 lines of evalog.txt and log.txt
                String evalogPath ="D:/GPFL/GPFL-master Embedding/data/" + dataset + "/ins3-car3/eval_log.txt";
                String logPath = "D:/GPFL/GPFL-master Embedding/data/" + dataset + "/ins3-car3/log.txt";

                try {
                    List<String> evalogLastLines = readLastLines(evalogPath, 7);
                    List<String> logLastLines = readLastLines(logPath, 7);

                    // Write results to result.txt and result1.txt
                    writeResults("evalog.txt", dataset, method, evalogLastLines);
                    writeResults("log.txt", dataset, method, logLastLines);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static List<String> readLastLines(String filePath, int numLines) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(filePath));
        int startIndex = lines.size() - numLines;
        return lines.subList(startIndex, lines.size());
    }

    private static void writeResults(String resultFile, String dataset, String method, List<String> lines) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(resultFile, true))) {
            writer.write("Dataset: " + dataset + ", Method: " + method + "\n");
            for (String line : lines) {
                writer.write(line + "\n");
            }
            writer.write("\n");
        }
    }
}

