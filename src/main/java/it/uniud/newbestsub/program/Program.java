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

                String datasetPath = commandLine.getOptionValue("f");

                datasetController = new DatasetController();
                datasetController.loadData(datasetPath);

            }

        } catch (ParseException exception) {
            System.out.println("EXCEPTION - There was something wrong with your command line options. Check the usage section below.");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "NewBestSub", options);
        }

        datasetController.solve();

        System.out.println("SYSTEM - NewBestSub is closing.");

    }

    public static Options loadCommandLineOptions() {

        Option source;
        Options options = new Options();
        source = Option.builder("f").longOpt( "file" ).desc( "Relative path to the CSV dataset file." ).hasArg().argName( "source" ).build();
        options.addOption(source);
        source = Option.builder("g").longOpt("gui").desc( "Indicates if the GUI should be started or not." ).build();
        options.addOption(source);
        return options;

    }

    /*public void start(Stage primaryStage) throws Exception {
        gui.firstRun(primaryStage);
    }*/

}
