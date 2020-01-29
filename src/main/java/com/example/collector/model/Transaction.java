package com.example.collector.model;

import lombok.Data;

/*
 * for demo purpose only
 */
@Data
public class Transaction {

	private long acctNum;
	
	private double trxAmt;
	
	private byte trxTypeId;
	
	private int trxDate; //yyyyMMdd
	
	private int trxTime; //hhmmss
		
	private long custId;
}
