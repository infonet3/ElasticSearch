/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.es.client;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import static org.elasticsearch.node.NodeBuilder.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

/**
 *
 * @author mattjones
 */
public class ElkMonitor {
    
    //getCount - Find the number of log entries which match the provided QUERY_STR
    public static long getCount(final Client ES_CLIENT, final String QUERY_STR) {

        CountResponse response = ES_CLIENT.prepareCount()
            .setQuery(QueryBuilders.queryString(QUERY_STR))
            .execute()
            .actionGet();

            return response.getCount();
    }
    
    //getMatchintItems - Retrieve the log entries which match the provided QUERY_STR
    public static void getMatchingItems(final Client ES_CLIENT, String QUERY_STR) {

        SearchResponse response = ES_CLIENT.prepareSearch()
            .setQuery(QueryBuilders.queryString(QUERY_STR))
            .execute()
            .actionGet();

        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());

            Map<String, Object> fieldMap = hit.getSource();
            Set<Entry<String, Object>> fieldSet = fieldMap.entrySet();
            for (Entry<String, Object> e : fieldSet) {
                System.out.println("Key = " + e.getKey());
                System.out.println("Value = " + e.getValue());
            }
            System.out.println("\n");

        }

    }
    
    public static void main(String... args) {

        final String ES_HOST_NAME = "localhost";
        final int ES_NODE_2_NODE_PORT = 9300;
        
        //Instantiating the next two classes implicitly pulls values from the elasticsearch.yml file
        try (Node node = nodeBuilder().node();    
            Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(ES_HOST_NAME, ES_NODE_2_NODE_PORT));) {
        
            String queryStr = "body:Michelle AND code:5";
            
            long matchingLogs = getCount(client, queryStr);
            System.out.println("Count = " + matchingLogs);
            
            getMatchingItems(client, queryStr);
        
            
        } catch (Exception exc) {
            System.out.println(exc.toString());
        }
        
    }
 
}
