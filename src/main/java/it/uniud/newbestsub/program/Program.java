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

        try {

            parser = new DefaultParser();
            commandLine = parser.parse( options, arguments);

            if(commandLine.hasOption("f")) {

                if(commandLine.hasOption("c")){

                    if(commandLine.getOptionValue("c").equals("Pearson")||commandLine.getOptionValue("c").equals("Kendall")){

                        String datasetPath = commandLine.getOptionValue("f");
                        String chosenCorrelationMethod = commandLine.getOptionValue("c");
                        datasetController = new DatasetController();
                        datasetController.loadData(datasetPath);
                        datasetController.solve(chosenCorrelationMethod);

                    } else {
                        throw new ParseException("EXCEPTION - The value for the option <<c>> or <<corr>> is wrong. Check the usage section below.");
                    }

                } else {
                    throw new ParseException("EXCEPTION - You have to specify a method to compute correlations. Check the usage section below.");
                }

            } else {
                throw new ParseException("EXCEPTION - You have to specify a path to a source file. Check the usage section below");
            }

        } catch (ParseException exception) {
            System.out.println(exception.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "NewBestSub", options);
        }

        System.out.println("SYSTEM - NewBestSub is closing.");

    }

    public static Options loadCommandLineOptions() {

        Option source;
        Options options = new Options();
        source = Option.builder("f").longOpt( "file" ).desc( "Relative path to the CSV dataset file." ).hasArg().argName( "source" ).build();
        options.addOption(source);
        source = Option.builder("c").longOpt("corr").desc( "Indicates the method that must be used to compute correlations.").hasArg().argName("Pearson,Kendall").build();
        options.addOption(source);
        source = Option.builder("g").longOpt("gui").desc( "Indicates if the GUI should be started or not." ).build();
        options.addOption(source);
        return options;

    }

    /*public void start(Stage primaryStage) throws Exception {
        gui.firstRun(primaryStage);
    }*/

}
