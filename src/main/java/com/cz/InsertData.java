package com.cz;

import org.apache.kudu.client.*;

public class InsertData {
    public static void main(String[] args) throws KuduException {
        String tableName = "bigData";
        KuduClient client = KuduUtil.getClient();
        KuduTable table = null;
        client.openTable(tableName);// 获取table
        // 获取一个会话
        KuduSession session = KuduUtil.getSession(client);
        long startTime = System.currentTimeMillis();
        int val = 0;
        for (int i = 0; i < 60; i++){
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            row.addLong(0,1);
            row.addLong(1,i*1000);
            row.addLong(2,i);
            row.addString(3,"bigData");
            session.apply(insert);
            if (val % 10 ==0){
                session.flush();
                val = 0;
            }
            val ++;
        }
        session.flush();
        long endTime = System.currentTimeMillis();
        System.out.println("the timePeriod executed is : " + (endTime - startTime) + "ms");
        session.close();
    }
}
