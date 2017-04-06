package program;

import dataset.DatasetController;
import dataset.DatasetModel;
import dataset.DatasetView;

import org.apache.commons.cli.*;

public class Program {

    DatasetModel datasetModel;
    DatasetView datasetView;
    DatasetController datasetController;

    public static void main(String[] arguments) {

        // launch(args);

        CommandLine commandLine;
        CommandLineParser parser;

        Options options = loadCommandLineOptions();

        try {
            parser = new DefaultParser();
            commandLine = parser.parse( options, arguments);

            if(commandLine.hasOption("f")) {

                String datasetPath = commandLine.getOptionValue("f");

                DatasetController datasetController = new DatasetController();
                datasetController.loadData(datasetPath);

            } else {

                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp( "NewBestSub", options);

            }

        } catch (ParseException exception) {
            System.out.println(exception.getMessage());
        }

    }

    public static Options loadCommandLineOptions() {

        Options options = new Options();
        Option source = Option.builder("f").longOpt( "file" ).desc( "Relative path to the CSV dataset file."  ).hasArg().argName( "source" ).build();
        options.addOption(source);
        return options;

    }

    /*public void start(Stage primaryStage) throws Exception {
        gui.firstRun(primaryStage);
    }*/

}
