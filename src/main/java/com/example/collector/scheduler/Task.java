package com.example.collector.scheduler;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.example.collector.constant.Common;
import com.example.collector.model.Transaction;

@Component
public class Task {
	private static final Logger LOGGER = LoggerFactory.getLogger(Task.class);

	@Value("${schedule.trxFile.path}")
    private String directory;
	
	@Value("${schedule.trxFile.fileType}")
    private String fileType;
	
	@Value("${schedule.trxFile.separatorRegex}")
    private String separatorRegex;
	
//	@Autowired
//	private TransactionService transactionService;

	@Scheduled(fixedRateString = "${schedule.trxFile.fixedRate}")
    public void readFilesFromDirectory() throws InterruptedException {
		try {
			LOGGER.debug("Read files from directory");
			
			File folder = new File(directory);
			FilenameFilter txtFileFilter = new FilenameFilter() {
	            @Override
	            public boolean accept(File dir, String name) {
	                if(name.endsWith(fileType)) {
	                    return true;
	                }
	                else {
	                    return false;
	                }
	            }
	        };
	 
	        File[] files = folder.listFiles(txtFileFilter);
	 
	        for (File file : files) {
	        	LOGGER.debug(file.getName());
	            fileHandling(file);
	            file.delete();
	        }
		} catch (Exception e) {
			LOGGER.error("Failed to run schedule, {}", e);
		}
	}
	
	private void fileHandling(File file) {
		try (Stream<String> lines = Files.lines(file.toPath()).skip(1)) {
			List<String> dataList = lines
				      .collect(Collectors.toList());
			
			List<Transaction> trxList = new ArrayList<Transaction>();
			for (String data : dataList) {
				try {
					LOGGER.info("data: " + data);
					Transaction trx = new Transaction();
					String[] contains = data.split(separatorRegex);
					
					for (int i = 0; i < contains.length; i++) {
						switch(i) {
							case 0: 
								trx.setAcctNum(Long. parseLong(contains[i]));
								break;
							case 1: 
								trx.setTrxAmt(Double.parseDouble(contains[i]));
								break;
							case 2: 
								trx.setTrxTypeId(Common.trxTypeMap.get(contains[i]));
								break;
							case 3: 
								trx.setTrxDate(Integer.parseInt(contains[i].replaceAll("-", "")));
								break;
							case 4: 
								trx.setTrxTime(Integer.parseInt(contains[i].replace(":", "")));
								break;
							case 5: 
								trx.setCustId(Long. parseLong(contains[i]));
								break;
						}
					}
					LOGGER.info("trx: " + trx);
					trxList.add(trx);
				} catch(Exception e) {
					LOGGER.error("Failed to parse file, {}", e);
				}
			}
			// transactionService.batchInsert(trxList);
		} 
		catch (IOException e) {
			LOGGER.error("Failed to read file, {}", e);
		}
	}
}