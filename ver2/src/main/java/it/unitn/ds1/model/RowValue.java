package it.unitn.ds1.model;

import lombok.Data;

@Data
public class RowValue {
	int version;
	int value;
	public RowValue(int version, int value) {
		this.version = version;
		this.value = value;
	}
	
}
