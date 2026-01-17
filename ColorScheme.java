package org.example.utils;

import com.itextpdf.kernel.colors.DeviceRgb;

public class ColorScheme {
    private static DeviceRgb hex(String hex) {
        java.awt.Color c = java.awt.Color.decode(hex);
        return new DeviceRgb(c.getRed(), c.getGreen(), c.getBlue());
    }

    // Option B: Modern Data Dashboard Color Scheme
    public  static DeviceRgb darkGreen = hex("#2E7D32");           // Title (darker shade of your green)
    public  static DeviceRgb labelColor = hex("#1B5E20");
    public static DeviceRgb tableHeaderColor =hex("#90EE90");
    public static DeviceRgb whiteColor =hex("#FFFFFF");
    public  static DeviceRgb charcoal = hex("#424242");            // Values/text
    public  static DeviceRgb lightGray = hex("#E0E0E0");
}
