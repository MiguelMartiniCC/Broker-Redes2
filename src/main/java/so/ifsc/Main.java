package so.ifsc;

import javax.swing.SwingUtilities;
import so.ifsc.View.Init;

public class Main {
    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            new Init().setVisible(true);
        });
    }
}