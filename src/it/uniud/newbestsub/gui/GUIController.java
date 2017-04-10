package it.uniud.newbestsub.gui;

import javafx.stage.Stage;

public class GUIController {

    public GUIModel model;
    public GUIView view;

    public GUIController() {
        model = new GUIModel();
        view  = new GUIView();
    }

    public void firstRun(Stage primaryStage) throws Exception{
        view.firstRun(primaryStage, model.getWindowName(), model.getTitle(), model.getHeight(), model.getWidth());
    }
}
