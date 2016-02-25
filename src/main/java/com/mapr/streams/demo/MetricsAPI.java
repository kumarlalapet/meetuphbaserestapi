package com.mapr.streams.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MetricsAPI {

    private static Configuration conf;
    private static final String TABLE_NAME = "/tables/streammetrics";

    static {
        conf = HBaseConfiguration.create();
    }
    
	@RequestMapping(produces = MediaType.APPLICATION_JSON_VALUE, value = "/metricsapi")
	public int getCount(@RequestParam(value = "prefix", defaultValue = "NM") String prefixStr,
			@RequestParam(value = "limit", defaultValue = "1") int limit) {
        int count = 0;

        HTable profileTable = null;
        try {

            profileTable = new HTable(conf, TABLE_NAME);

            byte[] prefix=Bytes.toBytes(prefixStr+"_10S");
            Scan scan = new Scan(prefix);
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            PrefixFilter prefixFilter = new PrefixFilter(prefix);
            filterList.addFilter(prefixFilter);
            filterList.addFilter(new PageFilter(limit));
            scan.setFilter(filterList);

            ResultScanner scanner = profileTable.getScanner (scan);

            for (Result result = scanner.next(); (result != null); result = scanner.next()) {
                for(KeyValue keyValue : result.list()) {
                    count = count + Integer.parseInt(Bytes.toString(keyValue.getValue()));
                }
            }

        } catch (IOException e) {
        	e.printStackTrace();
        } finally {
            if (profileTable != null) {
				try {
					profileTable.close();
				} catch (IOException e) {
				}
            }
        }
        return count;
	}
}
