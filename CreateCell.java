package org.example.utils;

import com.itextpdf.kernel.colors.DeviceRgb;
import com.itextpdf.kernel.font.PdfFont;
import com.itextpdf.layout.borders.Border;
import com.itextpdf.layout.element.Cell;
import com.itextpdf.layout.element.Paragraph;
import com.itextpdf.layout.properties.TextAlignment;

public class CreateCell {
    public static Cell headerCell(String text, PdfFont font, DeviceRgb bg) {
        return new Cell()
                .add(new Paragraph(text))
                .setFont(font)
                .setFontSize(10)
                .setTextAlignment(TextAlignment.CENTER)
                .setBackgroundColor(bg)
                .setPadding(6);
    }

    public static Cell emptyHeaderCell() {
        return new Cell()
                .setBorder(Border.NO_BORDER);
    }

    public static Cell rowLabelCell(String text, PdfFont font) {
        return new Cell()
                .add(new Paragraph(text))
                .setFont(font)
                .setFontSize(10)
                .setTextAlignment(TextAlignment.LEFT)
                .setPadding(6);
    }

    public static Cell valueCell(String text, PdfFont font, DeviceRgb color) {
        return new Cell()
                .add(new Paragraph(text))
                .setFont(font)
                .setFontSize(10)
                .setFontColor(color)
                .setTextAlignment(TextAlignment.CENTER)
                .setPadding(6);
    }

}
