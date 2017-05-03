package it.uniud.newbestsub.utils;

import java.io.File;
import java.io.IOException;

import java.util.logging.*;

public class BestSubsetLogger {

    public static BestSubsetLogger instance;
    public static FileHandler fileHandler;
    public static StreamHandler streamHandler;
    public static BestSubsetFormatter textFormatter;
    public static Logger logger;
    public static Level currentLevel;
    public static String modality;

    public BestSubsetLogger() {
    }

    public static BestSubsetLogger getInstance(String modalityToUse) {
        if (instance == null) {
            instance = new BestSubsetLogger(modalityToUse);
        }
        return instance;
    }

    public static BestSubsetLogger getInstance() {
        return instance;
    }

    private BestSubsetLogger(String modalityToUse) {

        modality = modalityToUse;
        logger = Logger.getLogger(this.getClass().getName());
        logger.setUseParentHandlers(false);

        switch (modality) {
            case "Debug":
                logger.setLevel(Level.FINE);
                currentLevel = Level.FINE;
                break;
            case "File":
                logger.setLevel(Level.FINER);
                currentLevel = Level.FINER;
        }

        File logDirectory = new File(Constants.LOG_PATH);
        if (!logDirectory.exists()) {
            logDirectory.mkdir();
        }

        try {
            fileHandler = new FileHandler(Constants.LOG_PATH + Constants.LOG_FILE_NAME);
        } catch (IOException exception) {
            System.out.println(exception.getMessage());
        }

        textFormatter = new BestSubsetFormatter();

        fileHandler.setFormatter(textFormatter);
        fileHandler.setLevel(Level.FINER);
        logger.addHandler(fileHandler);
        streamHandler = new StreamHandler(System.out, textFormatter);
        streamHandler.setFormatter(textFormatter);
        streamHandler.setLevel(Level.FINE);
        logger.addHandler(streamHandler);

    }

    public void log(String message) {
        logger.log(currentLevel, message);
    }

}