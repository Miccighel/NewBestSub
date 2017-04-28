package it.uniud.newbestsub.program;

import it.uniud.newbestsub.dataset.DatasetController;
import it.uniud.newbestsub.dataset.DatasetModel;
import it.uniud.newbestsub.dataset.DatasetView;

import org.apache.commons.cli.*;

public class Program {

    DatasetModel datasetModel;
    DatasetView datasetView;
    DatasetController datasetController;

    public static void main(String[] arguments) {

        // launch(args);

        System.out.println("SYSTEM - NewBestSub is starting.");

        CommandLine commandLine;
        CommandLineParser parser;

        Options options = loadCommandLineOptions();

        DatasetController datasetController = new DatasetController();

        String datasetPath = "";
        String resultPath = "";
        String chosenCorrelationMethod = "";
        String targetToAchieve = "";

        try {

            parser = new DefaultParser();
            commandLine = parser.parse(options, arguments);

            datasetPath = "data/" + commandLine.getOptionValue("fi") + ".csv";
            resultPath = commandLine.getOptionValue("fo");

            if (commandLine.getOptionValue("c").equals("Pearson") || commandLine.getOptionValue("c").equals("Kendall")) {
                chosenCorrelationMethod = commandLine.getOptionValue("c");
            } else {
                throw new ParseException("EXCEPTION (System) - You have to specify a target for the program. Check the usage section below.");
            }

            if (commandLine.getOptionValue("t").equals("Best") || commandLine.getOptionValue("t").equals("Worst") || commandLine.getOptionValue("t").equals("Average")) {
                targetToAchieve = commandLine.getOptionValue("t");
            } else {
                throw new ParseException("EXCEPTION (System) - Value for the option <<t>> or <<target>> is wrong. Check the usage section below.");
            }

            datasetController = new DatasetController();
            datasetController.loadData(datasetPath);
            datasetController.solve(chosenCorrelationMethod, targetToAchieve, resultPath);

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
        source = Option.builder("fi").longOpt("fileIn").desc("Relative path to the CSV dataset file (do not use any extension in filename).").hasArg().argName("SourceFile").required().build();
        options.addOption(source);
        source = Option.builder("fo").longOpt("fileOut").desc("Relative path to the output file (do not use any extension in file name).").hasArg().argName("OutputFile").required().build();
        options.addOption(source);
        source = Option.builder("c").longOpt("corr").desc("Indicates the method that must be used to compute correlations. Available methods: Pearson, Kendall. Choose one of them.").hasArg().argName("Method").required().build();
        options.addOption(source);
        source = Option.builder("t").longOpt("target").desc("Indicates the target that must be achieved. Available targets: Best, Worst, Average. Choose one of them.").hasArg().argName("Target").required().build();
        options.addOption(source);
        source = Option.builder("g").longOpt("gui").desc("Indicates if the GUI should be started or not.").build();
        options.addOption(source);
        return options;
    }

    /*public void start(Stage primaryStage) throws Exception {
        gui.firstRun(primaryStage);
    }*/

}
