package com.dataingestion.dataingestion.controller;


import com.dataingestion.dataingestion.ExcelReader.ExcelReaderService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URISyntaxException;
import java.nio.file.Paths;

@RestController
@RequestMapping("/loan")
public class LoanController {
    private final ExcelReaderService excelReaderService;

    public LoanController(ExcelReaderService excelReaderService) {
        this.excelReaderService = excelReaderService;
    }

    @PostMapping("/sendloan1")
    public String producerToLOAN_1() throws URISyntaxException {
        excelReaderService.readExcelAndSendToKafka(Paths.get(getClass().getClassLoader().getResource("loan1.xlsx").toURI()), "LOAN_1");
        return "Excel data sent to Kafka topic: " +
                "LOAN_1";
    }

    @PostMapping("/sendloan2")
    public String producerToLOAN_2() throws URISyntaxException {
        excelReaderService.readExcelAndSendToKafka(Paths.get(getClass().getClassLoader().getResource("loan2.xlsx").toURI()), "LOAN_2");
        return "Excel data sent to Kafka topic: " +
                "LOAN_2";
    }
}
