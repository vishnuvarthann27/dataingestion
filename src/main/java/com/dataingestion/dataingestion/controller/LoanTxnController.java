package com.dataingestion.dataingestion.controller;

import com.dataingestion.dataingestion.ExcelReader.ExcelReaderService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URISyntaxException;
import java.nio.file.Paths;

@RestController
@RequestMapping("/loan-txn")
public class LoanTxnController {
    private final ExcelReaderService excelReaderService;

    public LoanTxnController(ExcelReaderService excelReaderService) {
        this.excelReaderService = excelReaderService;
    }

    @PostMapping("/sendloantxn1")
    public String producerToLOAN_TXN_1() throws URISyntaxException {
        excelReaderService.readExcelAndSendToKafka(Paths.get(getClass().getClassLoader().getResource("loantxn1.xlsx").toURI()), "LOAN_TXN_1");
        return "Excel data sent to Kafka topic: " +
                "LOAN_TXN_1";
    }

    @PostMapping("/sendloantxn2")
    public String producerToLOAN_TXN_2() throws URISyntaxException {
        excelReaderService.readExcelAndSendToKafka(Paths.get(getClass().getClassLoader().getResource("loantxn2.xlsx").toURI()), "LOAN_TXN_2");
        return "Excel data sent to Kafka topic: " +
                "LOAN_TXN_2";
    }
}
