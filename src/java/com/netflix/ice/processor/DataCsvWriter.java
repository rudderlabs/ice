package com.netflix.ice.processor;

import java.io.IOException;
import java.io.OutputStreamWriter;

public class DataCsvWriter extends DataFile {
    private ReadWriteData data;

	public DataCsvWriter(String name, ReadWriteData data, boolean compress) throws Exception {
    	super(name, compress);
    	this.data = data;
	}

	@Override
	protected void write() throws IOException {
    	OutputStreamWriter writer = new OutputStreamWriter(os);
        try {
        	ReadWriteData.Serializer.serializeCsv(writer, data);
        	writer.flush();
        }
        finally {
        	writer.close();
        }
	}
}
