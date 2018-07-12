package com.netflix.ice.processor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;

public interface ReadWriteDataSerializer {
    public void serialize(DataOutput out) throws IOException;

    public void deserialize(AccountService accountService, ProductService productService, DataInput in) throws IOException;
}
