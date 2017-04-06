package gui;

import dataset.DatasetController;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

public class GUIView {

    public GUIView() {}

    public void firstRun(Stage primaryStage, String windowName, String title, int height, int width) throws Exception {
        Parent root = FXMLLoader.load(getClass().getResource(windowName));
        primaryStage.setTitle(title);
        primaryStage.setScene(new Scene(root, width, height));
        primaryStage.show();
    }
}
