package it.unitn.ds1.model;

import java.util.HashMap;
import java.util.Map;

import lombok.Data;

@Data
public class PrivateWorkspace {
	private Map<Integer, RowValue> data;

	public PrivateWorkspace() {
		this.data = new HashMap<>();
	}
	
}
