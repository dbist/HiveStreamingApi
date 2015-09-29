/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hortonworks.hive;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hive.hcatalog.streaming.ConnectionError;
import org.apache.hive.hcatalog.streaming.DelimitedInputWriter;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.ImpersonationFailed;
import org.apache.hive.hcatalog.streaming.InvalidColumn;
import org.apache.hive.hcatalog.streaming.InvalidPartition;
import org.apache.hive.hcatalog.streaming.InvalidTable;
import org.apache.hive.hcatalog.streaming.PartitionCreationFailed;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.TransactionBatch;

/**
 *
 * @author artem
 */
public class HiveDumpLargeBatch {

    private static String dbName = "testing";
    private static String tblName = "alerts";
    private static final ArrayList<String> partitionVals = new ArrayList<>(2);

    private static final String serdeClass = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    private static final Logger LOG = Logger.getLogger(HiveDumpLargeBatch.class.getName());

    public static void main(String[] args) {
        try {
            long start = System.currentTimeMillis();
            partitionVals.add("Asia");
            partitionVals.add("India");

            String[] fieldNames = "id,msg".split(",");

            HiveEndPoint hiveEP = new HiveEndPoint("thrift://sandbox.hortonworks.com:9083", dbName, tblName, partitionVals);
            StreamingConnection connection = hiveEP.newConnection(true);
            DelimitedInputWriter writer
                    = new DelimitedInputWriter(fieldNames, ",", hiveEP);
            TransactionBatch txnBatch = connection.fetchTransactionBatch(10, writer);

            LOG.info(txnBatch.getCurrentTransactionState().toString());
            LOG.info(String.format("%d", txnBatch.getCurrentTxnId()));
            LOG.info("begin transaction");
            txnBatch.beginNextTransaction();
            
            int count = 0;
            List<byte[]> batch = new ArrayList<>();
            for(int i=0; i < 1000000; i++) {
                batch.add((i+",count").getBytes());
                
                if((count % 10000) == 0) {
                    LOG.info("Commiting: " + count);
                    txnBatch.commit();
                    batch.clear();
                }
            }
            txnBatch.commit();
            LOG.info("end transaction");
            LOG.info(txnBatch.getCurrentTransactionState().toString());
            LOG.info(String.format("%d", txnBatch.getCurrentTxnId()));
            txnBatch.close();
            long end = System.currentTimeMillis();
            LOG.info(String.format("Finished in: %d sec", (end - start)/1000));

        } catch (ConnectionError ex) {
            LOG.log(Level.SEVERE, null, ex);
        } catch (InvalidPartition ex) {
            LOG.log(Level.SEVERE, null, ex);
        } catch (InvalidTable ex) {
            LOG.log(Level.SEVERE, null, ex);
        } catch (PartitionCreationFailed ex) {
            LOG.log(Level.SEVERE, null, ex);
        } catch (ImpersonationFailed ex) {
            LOG.log(Level.SEVERE, null, ex);
        } catch (InterruptedException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
        } catch (InvalidColumn ex) {
            LOG.log(Level.SEVERE, null, ex);
        } catch (StreamingException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
    }
}
