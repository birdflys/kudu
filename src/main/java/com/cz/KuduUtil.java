package com.cz;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.SessionConfiguration;


public class KuduUtil {

    public static KuduClient getClient(){
        KuduClient client = new KuduClient.KuduClientBuilder("master.com")
                .defaultAdminOperationTimeoutMs(60000).build();
        KuduSession session = client.newSession();
        return client;
    }

    public static KuduSession getSession(KuduClient client){
        KuduSession session = client.newSession();
        session.setTimeoutMillis(60000);// 此处所定义的是rpc连接超时
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace(10000);
        return session;
    }
}
