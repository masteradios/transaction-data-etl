package org.example.utils;

import com.itextpdf.kernel.colors.DeviceRgb;
import com.itextpdf.kernel.font.PdfFont;
import com.itextpdf.layout.Document;
import com.itextpdf.layout.element.*;
import com.itextpdf.layout.properties.*;

import java.util.List;

import static org.example.utils.CreateCell.*;


public class TableUtility {


    public static void createSummaryTable(
            Document document,
            PdfFont font,
            String tableName,
            List<String> inputLines,
            String percentageHeader
    ) {



        Div section = new Div()
                .setKeepTogether(true)
                .setMarginTop(20);
    /* =========================
       SECTION TITLE
       ========================= */
        Paragraph title = new Paragraph(tableName)
                .setFont(font)
                .setFontSize(14)
                .setMarginTop(20)
                .setPaddingLeft(5)
                .setBackgroundColor(ColorScheme.tableHeaderColor)
                .setFontColor(ColorScheme.whiteColor)
                .setMarginBottom(0);
        section.add(title);

        //document.add(title);

    /* =========================
       TABLE STRUCTURE
       ========================= */
        float[] columnWidths = {3, 2, 2, 2, 2};
        Table table = new Table(columnWidths).setWidth(UnitValue.createPercentValue(100))
                .setMarginTop(0.9F);
        table.setWidth(UnitValue.createPercentValue(100));

        DeviceRgb headerBg = new DeviceRgb(240, 240, 240);
        DeviceRgb textColor = new DeviceRgb(66, 66, 66);
        DeviceRgb green = new DeviceRgb(46, 125, 50);
        DeviceRgb red = new DeviceRgb(211, 47, 47);

    /* =========================
       HEADER ROW
       ========================= */
        table.addCell(headerCell("%Accepted".equals(percentageHeader)?"":"Attribute",font,headerBg));
        table.addCell(headerCell("Given", font, headerBg));
        table.addCell(headerCell("%Accepted".equals(percentageHeader)?"Accepted":"Valid", font, headerBg));
        table.addCell(headerCell("%Accepted".equals(percentageHeader)?"Rejected":"Invalid/Blank", font, headerBg));
        table.addCell(headerCell(percentageHeader, font, headerBg));

    /* =========================
       DATA ROW
       ========================= */

        for(String line :inputLines){

            String fieldName= line.split(",")[0];
            Integer accepted = Integer.valueOf(line.split(",")[1]);
            Integer rejected = Integer.valueOf(line.split(",")[2]);
            Integer given=accepted+rejected;
            // Calculate percentage
            double percentAccepted = given == 0 ? 0 : ((double) accepted / given) * 100;

            table.addCell(rowLabelCell(fieldName, font));
            table.addCell(valueCell(String.valueOf(given), font, textColor));
            table.addCell(valueCell(String.valueOf(accepted), font, green));
            table.addCell(valueCell(String.valueOf(rejected), font, red));
            table.addCell(valueCell(String.format("%.2f%%", percentAccepted), font, green));
        }



        section.add(table);
        document.add(section);
    }

    public static void createTable(
            Document document,
            PdfFont font,
            String tableName,
            List<String> rejectionLines

    ) {



        Div section = new Div()
                .setKeepTogether(true)
                .setMarginTop(20);
    /* =========================
       SECTION TITLE
       ========================= */
        Paragraph title = new Paragraph(tableName)
                .setFont(font)
                .setFontSize(14)
                .setMarginTop(20)
                .setPaddingLeft(5)
                .setBackgroundColor(ColorScheme.tableHeaderColor)
                .setFontColor(ColorScheme.whiteColor)
                .setMarginBottom(0);
        section.add(title);

        //document.add(title);

    /* =========================
       TABLE STRUCTURE
       ========================= */
        float[] columnWidths = {3, 2};
        Table table = new Table(columnWidths).setWidth(UnitValue.createPercentValue(100))
                .setMarginTop(0.9F);
        table.setWidth(UnitValue.createPercentValue(100));

        DeviceRgb headerBg = new DeviceRgb(240, 240, 240);
        DeviceRgb textColor = new DeviceRgb(66, 66, 66);
        DeviceRgb green = new DeviceRgb(46, 125, 50);
        DeviceRgb red = new DeviceRgb(211, 47, 47);

    /* =========================
       HEADER ROW
       ========================= */
        table.addCell(headerCell("Reject Reason", font, headerBg));
        table.addCell(headerCell("Number of Records Rejected", font, headerBg));

    /* =========================
       DATA ROW
       ========================= */
        if(rejectionLines.isEmpty()){

            Cell notAvailableCell = new Cell(1, 2) // 1 row, 2 columns
                    .add(new Paragraph("Not Available"))
                    .setFont(font)
                    .setFontSize(10)
                    .setTextAlignment(TextAlignment.CENTER)
                    .setVerticalAlignment(VerticalAlignment.MIDDLE)
                    .setPadding(10);

            table.addCell(notAvailableCell);

        }
        else {

            for (String line : rejectionLines) {
                String lineName = line.split(",")[0];

                Integer rejectCount = Integer.valueOf(line.split(",")[1]);
                System.out.println("Key: " + lineName + ", Value: " + rejectCount);

                table.addCell(rowLabelCell(lineName, font));
                table.addCell(valueCell(String.valueOf(rejectCount), font, green));
            }
        }
        section.add(table);
        document.add(section);
    }




}




