package com.dataingestion.dataingestion.ExcelReader;

import com.dataingestion.dataingestion.KafkaProducer.KafkaProducerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MultiGauge;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
public class ExcelReaderService {
    private final KafkaProducerService kafkaProducerService;
    private final ObjectMapper objectMapper = new ObjectMapper(); // JSON Converter
    private final SimpleDateFormat dateTimeFormat  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public ExcelReaderService(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    public void readExcelAndSendToKafka(Path filePath, String topic, long delay) {
        try (FileInputStream fis = new FileInputStream(filePath.toFile());
             Workbook workbook = new XSSFWorkbook(fis)) {

            Sheet sheet = workbook.getSheetAt(0); // Read first sheet
            Iterator<Row> rowIterator = sheet.iterator();

            // Get headers from the first row
            Row headerRow = rowIterator.next();
            List<String> headers = new ArrayList<>();
            for (Cell cell : headerRow) {
                headers.add(cell.getStringCellValue());
            }

            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                Map<String, Object> rowData = new LinkedHashMap<>();

                for (int i = 0; i < headers.size(); i++) {
                    Cell cell = row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                    rowData.put(headers.get(i), getCellValue(cell));
                }

                rowData.put("created_timestamp", dateTimeFormat .format(new Date()));

                String jsonMessage = objectMapper.writeValueAsString(rowData);
                Object loanNumberObj = rowData.get("invstr_loan_nbr");
                String loanNumber = (loanNumberObj != null) ? loanNumberObj.toString() : null;

                if (loanNumber != null) {
                    kafkaProducerService.sendMessage(topic, loanNumber, jsonMessage);
                    System.out.println("Sent JSON to Kafka: " + jsonMessage);
                } else {
                    System.out.println("Missing invstr_loan_nbr for row, skipping message send.");
                }

                Thread.sleep(delay);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void readExcelAndSendToKafkafile(String filePath, String topic, long delay) {
        try (InputStream inputStream = getClass().getResourceAsStream(filePath)) {
            assert inputStream != null;
            try (Workbook workbook = new XSSFWorkbook(inputStream)) {

                Sheet sheet = workbook.getSheetAt(0); // Read first sheet
                Iterator<Row> rowIterator = sheet.iterator();

                // Get headers from the first row
                Row headerRow = rowIterator.next();
                List<String> headers = new ArrayList<>();
                for (Cell cell : headerRow) {
                    headers.add(cell.getStringCellValue());
                }

                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    Map<String, Object> rowData = new LinkedHashMap<>();

                    for (int i = 0; i < headers.size(); i++) {
                        Cell cell = row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                        rowData.put(headers.get(i), getCellValue(cell));
                    }

                    rowData.put("created_timestamp", dateTimeFormat .format(new Date()));

                    String jsonMessage = objectMapper.writeValueAsString(rowData);
                    Object loanNumberObj = rowData.get("invstr_loan_nbr");
                    String loanNumber = (loanNumberObj != null) ? loanNumberObj.toString() : null;

                    if (loanNumber != null) {
                        kafkaProducerService.sendMessage(topic, loanNumber, jsonMessage);
                        System.out.println("Sent JSON to Kafka: " + jsonMessage);
                    } else {
                        System.out.println("Missing invstr_loan_nbr for row, skipping message send.");
                    }

                    Thread.sleep(delay);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Object getCellValue(Cell cell) {
        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) { // Check if it's a DATE
                    return dateFormat.format(cell.getDateCellValue()); // Convert to "yyyy-MM-dd"
                }
                return cell.getNumericCellValue();
            case BOOLEAN:
                return cell.getBooleanCellValue();
            case FORMULA:
                return cell.getCellFormula();
            case BLANK:
                return "";
            default:
                return cell.toString();
        }
    }
}
