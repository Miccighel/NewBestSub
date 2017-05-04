package it.uniud.newbestsub.program;

import it.uniud.newbestsub.dataset.DatasetController;
import it.uniud.newbestsub.dataset.DatasetModel;
import it.uniud.newbestsub.dataset.DatasetView;

import it.uniud.newbestsub.utils.BestSubsetLogger;
import it.uniud.newbestsub.utils.Constants;
import org.apache.commons.cli.*;

import java.io.File;

public class Program {

    DatasetModel datasetModel;
    DatasetView datasetView;
    DatasetController datasetController;

    public static void main(String[] arguments) {

        // launch(args);

        CommandLine commandLine;
        CommandLineParser parser;

        Options options = loadCommandLineOptions();

        DatasetController datasetController = new DatasetController();

        String datasetPath = "";
        String resultPath = "";
        String chosenCorrelationMethod = "";
        String targetToAchieve = "";
        int numberOfIterations = 0;
        String loggingModality = "";
        BestSubsetLogger logger = null;

        try {

            parser = new DefaultParser();
            commandLine = parser.parse(options, arguments);
            datasetPath = Constants.INPUT_PATH + commandLine.getOptionValue("fi") + ".csv";
            resultPath = commandLine.getOptionValue("fi") + "-";

            if (commandLine.getOptionValue("l").equals("Debug") || commandLine.getOptionValue("l").equals("File")) {
                loggingModality = commandLine.getOptionValue("l");
                logger = BestSubsetLogger.getInstance(loggingModality);
                System.out.println("SYSTEM - NewBestSub is starting.");
                System.out.println("SYSTEM - Logging path is : \"" + Constants.LOG_PATH + Constants.LOG_FILE_NAME + "\"");
                System.out.println("SYSTEM - Wait for the program to complete...");
            } else {
                throw new ParseException("Value for the option <<l>> or <<log>> is wrong. Check the usage section below.");
            }

            if (commandLine.getOptionValue("c").equals("Pearson") || commandLine.getOptionValue("c").equals("Kendall")) {
                chosenCorrelationMethod = commandLine.getOptionValue("c");
                resultPath = resultPath + chosenCorrelationMethod + "-";
            } else {
                throw new ParseException("Value for the option <<c>> or <<corr>> is wrong. Check the usage section below.");
            }

            if (commandLine.getOptionValue("t").equals("Best") || commandLine.getOptionValue("t").equals("Worst") || commandLine.getOptionValue("t").equals("Average")) {
                if (commandLine.getOptionValue("t").equals("Average")) {
                    if (commandLine.hasOption("i")) {
                        throw new ParseException("Option <<i>> or <<iter>> is not necessary. Please remove it and launch the program again. Check the usage section below.");
                    }
                }
                targetToAchieve = commandLine.getOptionValue("t");
                resultPath += targetToAchieve;
                if (commandLine.hasOption("i") && !commandLine.getOptionValue("t").equals("Average")) {
                    try {
                        numberOfIterations = Integer.parseInt(commandLine.getOptionValue("i"));
                        datasetController = new DatasetController();
                        datasetController.loadData(datasetPath);
                        datasetController.solve(chosenCorrelationMethod, targetToAchieve, numberOfIterations, resultPath);
                    } catch (NumberFormatException exception) {
                        throw new ParseException("Value for the option <<i>> or <<iter>> is not an integer. Check the usage section below");
                    }
                } else {
                    if (commandLine.getOptionValue("t").equals("Best") || commandLine.getOptionValue("t").equals("Worst")) {
                        throw new ParseException("Value for the option <<i>> or <<iter>> is missing. Check the usage section below.");
                    } else {
                        datasetController = new DatasetController();
                        datasetController.loadData(datasetPath);
                        datasetController.solve(chosenCorrelationMethod, targetToAchieve, 0, resultPath);
                    }
                }

            } else {
                throw new ParseException("Value for the option <<t>> or <<target>> is wrong. Check the usage section below.");
            }
        } catch (ParseException exception) {
            System.out.println("EXCEPTION (System) - " + exception.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("NewBestSub", options);
        }

        System.out.println("SYSTEM - NewBestSub is closing.");
    }

    public static Options loadCommandLineOptions() {

        Option source;
        Options options = new Options();
        source = Option.builder("fi").longOpt("fileIn").desc("Relative path to the CSV dataset file (do not use any extension in filename) [REQUIRED].").hasArg().argName("SourceFile").required().build();
        options.addOption(source);
        source = Option.builder("c").longOpt("corr").desc("Indicates the method that must be used to compute correlations. Available methods: Pearson, Kendall. [REQUIRED]").hasArg().argName("Method").required().build();
        options.addOption(source);
        source = Option.builder("t").longOpt("target").desc("Indicates the target that must be achieved. Available targets: Best, Worst, Average. [REQUIRED]").hasArg().argName("Target").required().build();
        options.addOption(source);
        source = Option.builder("l").longOpt("log").desc("Indicates the required level of loggin. Available levels: Debug, File. [REQUIRED]").required().hasArg().argName("Log Level").build();
        options.addOption(source);
        source = Option.builder("i").longOpt("iter").desc("Indicates the number of iterations to be done. It must be an integer value. It is mandatory only if the selected target is: Best, Worst. [OPTIONAL]").hasArg().argName("Number").build();
        options.addOption(source);
        source = Option.builder("g").longOpt("gui").desc("Indicates if the GUI should be started or not. [OPTIONAL]").build();
        options.addOption(source);
        return options;

    }

    /*public void start(Stage primaryStage) throws Exception {
        gui.firstRun(primaryStage);
    }*/

}
