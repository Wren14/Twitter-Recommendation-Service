package edu.cmu.cc.group.q1;

import java.util.*;

import org.json.*;

public class Blockchain {
	
	private final List<Block> blocks;
	
	public Blockchain(JSONArray jsonInput) throws JSONException {
		
		final int chainLength = jsonInput.length();
		
		blocks = new ArrayList<>(chainLength + 1);
		
		for (int i = 0; i < chainLength; i++) {
			JSONObject blockJson = jsonInput.getJSONObject(i);
			Block block = new Block(blockJson);
			blocks.add(block);
		}
		
	}
	
	public List<Block> getBlocks () {
		return blocks;
	}
	

}
