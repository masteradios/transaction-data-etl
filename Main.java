package org.example;

import io.dagster.pipes.PipesContext;
import io.dagster.pipes.PipesContextImpl;
import io.dagster.pipes.loaders.PipesDefaultContextLoader;
import io.dagster.pipes.loaders.PipesEnvVarParamsLoader;
import io.dagster.pipes.writers.PipesDefaultMessageWriter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import com.itextpdf.kernel.pdf.PdfDocument;
import com.itextpdf.kernel.pdf.PdfWriter;
import com.itextpdf.kernel.font.PdfFont;
import com.itextpdf.kernel.font.PdfFontFactory;
import com.itextpdf.kernel.pdf.event.AbstractPdfDocumentEvent;
import com.itextpdf.kernel.pdf.event.AbstractPdfDocumentEventHandler;
import com.itextpdf.kernel.pdf.event.PdfDocumentEvent;
import com.itextpdf.kernel.geom.Rectangle;
import com.itextpdf.kernel.pdf.PdfPage;
import com.itextpdf.kernel.pdf.canvas.PdfCanvas;
import com.itextpdf.layout.Document;
import com.itextpdf.layout.Canvas;
import com.itextpdf.layout.element.Image;
import org.example.utils.TableUtility;

import java.util.*;

import static org.example.utils.ColorScheme.*;
import com.itextpdf.layout.element.Paragraph;
import com.itextpdf.layout.properties.TextAlignment;
import com.itextpdf.io.image.ImageDataFactory;

public class Main {
    public static void main(String[] args) {
        String inputFile = System.getProperty("fileId");
        String merchant_id = System.getProperty("merchantId");
        String merchant_name= System.getProperty("merchantName");
        String business_type = System.getProperty("businessType");
        String merchant_state= System.getProperty("merchantState");
        String baseDir = "/home/ubuntu/dagsterProjects/dagster_Test/summaryFiles";

        List<String> headers= List.of(
                inputFile,
                merchant_id,
                merchant_name,
                business_type,
                merchant_state);
        Path filePath = Paths.get(baseDir, inputFile);
        System.out.println("Input file: " + inputFile);
        try (PipesContext context = new PipesContextImpl(
                new PipesEnvVarParamsLoader(),
                new PipesDefaultContextLoader(),
                new PipesDefaultMessageWriter())) {

            context.getLogger().info("Starting PDF generation...");

            Map<String, Object> startEvent = new HashMap<>();
            startEvent.put("event", "generation_started");
            startEvent.put("timestamp", System.currentTimeMillis());
            context.reportCustomMessage(startEvent);

            Map<String, String> summaryData = readSummaryFile(context,headers,filePath, inputFile);


            int maxKeyLength = summaryData.keySet().stream()
                    .mapToInt(String::length)
                    .max()
                    .orElse(20);

            context.getLogger().info("========================================");
            for (Map.Entry<String, String> entry : summaryData.entrySet()) {
                String formattedLine = String.format("%-" + maxKeyLength + "s : %s",
                        entry.getKey(),
                        entry.getValue());
                context.getLogger().info(formattedLine);
            }

            context.getLogger().info("========================================");

            // Create metadata for Dagster UI
            Map<String, Object> metadata = new HashMap<>();

            // Create a markdown table with just the key-value pairs
            StringBuilder markdownTable = new StringBuilder();
            markdownTable.append("## Summary Data\n\n");
            markdownTable.append("| Field | Value |\n");
            markdownTable.append("|-------|-------|\n");

            for (Map.Entry<String, String> entry : summaryData.entrySet()) {
                markdownTable.append(String.format("| %s | %s |\n",
                        entry.getKey(),
                        entry.getValue()));
            }

            metadata.put("summary_table", markdownTable.toString());

            // Report to Dagster
            context.reportCustomMessage(metadata);
            //context.reportAssetMaterialization(metadata, null, null);

            // Finally, report the asset materialization with metadata
//            Map<String, Object> metadata = new HashMap<>();
//            metadata.put("records_processed", recordCount);
//            metadata.put("pages_generated", pageCount);
//            metadata.put("output_path", pdfPath);
//            context.reportCustomMessage(metadata);
            //context.reportAssetMaterialization(metadata, null, null);

            context.getLogger().info("PDF generation complete!");

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


    private static void parseSummaryLine(String line, Map<String, String> summaryData) {
        if (line == null || line.trim().isEmpty()) return;

        // Assuming format: "field,total,accepted" or "field|total|accepted"
        String[] parts = line.split("[,|]");  // Split by comma or pipe

        if (parts.length >= 3) {
            summaryData.put("summary_field", parts[0].trim());
            summaryData.put("summary_total", parts[1].trim());
            summaryData.put("summary_accepted", parts[2].trim());
        }
    }

    // Helper method to parse field lines
    private static void parseFieldLine(String line, Map<String, String> summaryData) {
        if (line == null || line.trim().isEmpty()) return;

        // Assuming format: "field_name,total_count,valid_count"
        String[] parts = line.split("[,|]");

        if (parts.length >= 3) {
            String fieldName = parts[0].trim();
            summaryData.put(fieldName + "_total", parts[1].trim());
            summaryData.put(fieldName + "_valid", parts[2].trim());
        }
    }

    public static Map<String, String> readSummaryFile(PipesContext context,List<String> headers,Path inputFilePath, String inputFileName) throws Exception {
        String baseName = inputFileName.endsWith(".csv")
                ? inputFileName.substring(0, inputFileName.length() - 4)
                : inputFileName;

        String dest = "/home/ubuntu/dagsterProjects/dagster_Test/reportPdfs/"
                + baseName + ".pdf";        Map<String, String> summaryData = new HashMap<>();
        Path start = Paths.get(inputFilePath.toUri());
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:**/*.csv");


        try (Stream<Path> stream = Files.walk(start)) {
            // Find the first matching path
            Optional<Path> firstFile =           stream
                    .filter(Files::isRegularFile)
                    .filter(matcher::matches)
                    .max(Comparator.comparingLong(p -> {
                        try {
                            return Files.getLastModifiedTime(p).toMillis();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }));

            // If a file was found, open the BufferedReader
            if (firstFile.isPresent()) {
                Path path = firstFile.get();
                context.getLogger().info("Processing first found file: " + path);

                try (BufferedReader reader = Files.newBufferedReader(path)) {
                    String line = "";
                    reader.readLine();
                    line = reader.readLine();
                    String summaryLine = line;
                    List<String> rejectionLines = new ArrayList<>();
                    List<String> fieldLines = new ArrayList<>();
                    parseSummaryLine(summaryLine, summaryData);

                    while ((line = reader.readLine()) != null) {
                        if (line.trim().isEmpty()) {
                            continue;
                        }

                        if (line.startsWith("Rejected due")) {
                            rejectionLines.add(line);

                        } else {
                            fieldLines.add(line);
                            parseFieldLine(line, summaryData);
                        }


                    }

                    createPdf(headers,dest, fieldLines, summaryLine, rejectionLines);
                } catch (Exception e) {
                    System.out.println(e.getCause());
                    System.out.println(e.getMessage());
                    context.getLogger().error("Error creating pdf: " + e.getMessage());
                }
            } else {
                System.out.println("No CSV files found in the directory.");
            }

        } catch (IOException e) {
            System.err.println("Error accessing directory: " + e.getMessage());
        }


        return summaryData;
    }


    public static void createPdf(List<String> headers,String dest, List<String> fieldLines, String summaryLine, List<String> rejectionLines) throws IOException {
        // Create PDF
        PdfWriter writer = new PdfWriter(dest);
        PdfDocument pdf = new PdfDocument(writer);
        Document document = new Document(pdf);

        // Set top margin to prevent content from overlapping with header
        // Increased margin since header is now taller
        document.setTopMargin(110);

        // Load Poppins font
        String poppinsFontPath = "/home/ubuntu/fonts/Poppins-Regular.ttf";
        PdfFont font = PdfFontFactory.createFont(poppinsFontPath,
                PdfFontFactory.EmbeddingStrategy.PREFER_EMBEDDED);

        // Add header event handler to all pages

        pdf.addEventHandler(PdfDocumentEvent.END_PAGE, new HeaderEventHandler(font,headers));
        TableUtility.createSummaryTable(document, font, "Transactions Summary", List.of(summaryLine), "%Accepted");
        TableUtility.createTable(document, font, "Rejection Summary", rejectionLines);
        TableUtility.createSummaryTable(document, font, "Data Completeness Metrics", fieldLines, "%Valid");
        document.close();
        System.out.println("PDF created successfully with Poppins font and headers!");
    }


}


class HeaderEventHandler extends AbstractPdfDocumentEventHandler {
    private final PdfFont font;
    private final List<String> headers;

    public HeaderEventHandler(PdfFont font,List<String> headers) {
        this.font = font;
        this.headers = headers;
    }

    @Override
    protected void onAcceptedEvent(AbstractPdfDocumentEvent event) {
        PdfDocumentEvent docEvent = (PdfDocumentEvent) event;
        PdfPage page = docEvent.getPage();
        PdfDocument pdfDoc = docEvent.getDocument();

        Rectangle pageSize = page.getPageSize();

        float leftMargin = 28;
        float rightMargin = 36;

        PdfCanvas pdfCanvas = new PdfCanvas(
                page.newContentStreamBefore(),
                page.getResources(),
                pdfDoc
        );

    /* =========================
       Vertical positioning
       ========================= */
        float topY = pageSize.getTop();
        float infoStartY = topY - 40;
        float strokeY = infoStartY - 70;

    /* =========================
       LOGO (UNCHANGED)
       ========================= */
        try {
            Image logo = new Image(ImageDataFactory.create("/home/ubuntu/logo/logo.png"));
            logo.setHeight(60);
            // logo.setAutoScale(true);
            //logo.scaleAbsolute(120, 50);  // width, height
            logo.setFixedPosition(leftMargin, strokeY + 16);
//                logo.setFixedPosition(logoX, logoY);

            Rectangle logoRect = new Rectangle(leftMargin, strokeY +50, 100, 55);
            Canvas logoCanvas = new Canvas(pdfCanvas, logoRect);
            logoCanvas.add(logo);
            logoCanvas.close();
        } catch (Exception ignored) {}

    /* =========================
       TITLE (Top aligned + centered)
       ========================= */
        Rectangle titleRect = new Rectangle(
                leftMargin,
                topY - 20,
                pageSize.getWidth() - leftMargin - rightMargin,
                20
        );

        Canvas titleCanvas = new Canvas(pdfCanvas, titleRect);
        titleCanvas.add(
                new Paragraph("Data Submission Report")
                        .setFont(font)
                        .setFontColor(darkGreen)
                        .setFontSize(15)
                        .setTextAlignment(TextAlignment.CENTER)
        );
        titleCanvas.close();

        float fontSize = 10;
        float padding = 20;
        float rowHeight = 20;

/* =========================
   DATA
   ========================= */
        String[][] leftColumn = {
                {"Merchant Name :", headers.get(2)},
                {"Business-Type :",headers.get(3)},
                {"File Name :", headers.get(0)},

        };

        String[][] rightColumn = {
                {"Merchant Id :", headers.get(1)},
                {"Merchant State :", headers.get(4)},
                {"File Submission Date :", "01-Aug-2025"}
        };

/* =========================
   MEASURE LABEL WIDTHS
   ========================= */
        float maxLeftLabelWidth = 0;
        for (String[] row : leftColumn) {
            maxLeftLabelWidth = Math.max(
                    maxLeftLabelWidth,
                    font.getWidth(row[0], fontSize)
            );
        }

        float maxRightLabelWidth = 0;
        for (String[] row : rightColumn) {
            maxRightLabelWidth = Math.max(
                    maxRightLabelWidth,
                    font.getWidth(row[0], fontSize)
            );
        }

/* =========================
   X POSITIONS
   ========================= */
        float leftLabelX = leftMargin+60;
        float leftValueX = leftLabelX + maxLeftLabelWidth + padding;

        float columnGap = 60;

        float rightColumnStartX = (pageSize.getWidth() / 2) + columnGap;

        float rightLabelX = rightColumnStartX;
        float rightValueX = rightLabelX + maxRightLabelWidth + padding;

/* =========================
   Y POSITION
   ========================= */
        float startY = infoStartY-24;


/* =========================
   CANVAS
   ========================= */
        Canvas infoCanvas = new Canvas(
                pdfCanvas,
                new Rectangle(
                        leftMargin,
                        startY - (2 * rowHeight),
                        pageSize.getWidth() - leftMargin - rightMargin,
                        2 * rowHeight
                )
        );

/* =========================
   RENDER ROWS
   ========================= */

        int rows = Math.min(leftColumn.length, rightColumn.length);


        for (int i = 0; i < rows; i++) {
            float y = startY - (i * rowHeight);

            // LEFT column
            infoCanvas.showTextAligned(
                    new Paragraph(leftColumn[i][0])
                            .setFont(font)
                            .setFontColor(labelColor)
                            .setFontSize(fontSize),
                    leftLabelX,
                    y,
                    TextAlignment.LEFT
            );

            infoCanvas.showTextAligned(
                    new Paragraph(leftColumn[i][1])
                            .setFont(font)
                            .setFontColor(charcoal)
                            .setFontSize(fontSize),
                    leftValueX,
                    y,
                    TextAlignment.LEFT
            );

            // RIGHT column
            infoCanvas.showTextAligned(
                    new Paragraph(rightColumn[i][0])
                            .setFont(font)
                            .setFontColor(labelColor)
                            .setFontSize(fontSize),
                    rightLabelX,
                    y,
                    TextAlignment.LEFT
            );

            infoCanvas.showTextAligned(
                    new Paragraph(rightColumn[i][1])
                            .setFont(font)
                            .setFontColor(charcoal)
                            .setFontSize(fontSize),
                    rightValueX,
                    y,
                    TextAlignment.LEFT
            );
        }

        infoCanvas.close();
    /* =========================
       STROKE
       ========================= */
        pdfCanvas.setStrokeColor(lightGray)
                .moveTo(leftMargin+8, strokeY)
                .lineTo(pageSize.getRight() - rightMargin, strokeY)
                .stroke();
    }

    // Divider line

}