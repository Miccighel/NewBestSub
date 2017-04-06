package gui;

public class GUIModel {

    public String windowName;
    public String title;
    public int height;
    public int width;

    public GUIModel(){
        windowName = "sample.fxml";
        title = "Hello";
        height = 300;
        width = 400;
    }

    public String getWindowName() {
        return windowName;
    }

    public String getTitle() { return title; }

    public int getHeight() { return height; }

    public int getWidth() { return width; }
}
