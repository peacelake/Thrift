// package org.shirdrn.queryproxy.common;
// import java.io.Closeable;
// import org.shirdrn.queryproxy.thrift.protocol.QueryProxyService.Iface;
//
// public abstract class ConfiguredQueryService implements Iface, Closeable {
//      protected final Configurable context;
//      public ConfiguredQueryService(Configurable context) {
//           super();
//           this.context = context;
//      }
// }

package org.shirdrn.queryproxy.thrift.service.solr;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.thrift.TException;
import org.shirdrn.queryproxy.common.Configurable;
import org.shirdrn.queryproxy.common.ConfiguredQueryService;
import org.shirdrn.queryproxy.thrift.protocol.QueryFailureException;
import org.shirdrn.queryproxy.thrift.protocol.QueryParams;
import org.shirdrn.queryproxy.thrift.protocol.QueryResult;
import org.shirdrn.queryproxy.utils.ResultUtils;

public class SolrQueryService extends ConfiguredQueryService {

     private static final Log LOG = LogFactory.getLog(SolrQueryService.class);
     private CloudSolrServer solrServer;
     private static final String writerType = "json";

     public SolrQueryService(Configurable context) {
          super(context);
          String zkHost = context.get("query.proxy.solr.zkHost");
          try {
               solrServer = new CloudSolrServer(zkHost);
          } catch (MalformedURLException e) {
               throw new RuntimeException(e);
          }
     }

     @Override
     public QueryResult query(QueryParams params) throws QueryFailureException, TException {
          int offset = 0;
          int length = 10;
          Map<String,String> map = new HashMap<>();
          Iterator<String> iter = params.getParamListIterator();
          while(iter.hasNext()) {
               String kv = iter.next();
               if(kv != null) {
                    String[] items = kv.split("=");
                    if(items.length == 2) {
                         String key = items[0].trim();
                         String value = items[1].trim();
                         map.put(key, value);
                         if(key.equals(CommonParams.START)) {
                              offset = Integer.parseInt(value);
                         }
                         if(key.equals(CommonParams.ROWS)) {
                              length = Integer.parseInt(value);
                         }
                    }
               }
          }
          map.put("collection", params.getTable());
          map.put("wt", writerType);
          LOG.info("Solr params: " + map);

          // query using Solr
          QueryResponse response = null;
          SolrParams solrParams = new MapSolrParams(map);
          try {
               response = solrServer.query(solrParams);
          } catch (SolrServerException e) {
               LOG.error("Failed to query solr server: ", e);
               throw new QueryFailureException(e.toString());
          }

          // process result
          QueryResult result = new QueryResult();
          result.setOffset(offset);
          result.setLength(length);
          if(response != null) {
               result.setResults(ResultUtils.getJSONResults(response));
          }
          return result;
     }

     @Override
     public void close() throws IOException {
          solrServer.shutdown();
     }

}

private static final String KEY_VERSION = "_version_";
private static final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

public static List<String> getJSONResults(QueryResponse response) {
     ListIterator<SolrDocument> iter = response.getResults().listIterator();
     List<String> resultDocs = new ArrayList<String>();
     while(iter.hasNext()) {
          SolrDocument doc = iter.next();
          JSONObject jDoc = new JSONObject();
          Set<String> ks = doc.keySet();
          if(ks.contains(KEY_VERSION)) {
               ks.remove(KEY_VERSION);
          }
          for(String key : ks) {
               Object v = doc.getFieldValue(key);
               if(v instanceof Date) {
                    jDoc.put(key, DF.format((Date) v));
                    continue;
               }
               jDoc.put(key, v);
          }
          resultDocs.add(jDoc.toString());
     }
     return resultDocs;
}

package org.shirdrn.queryproxy.thrift.service.sql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.shirdrn.queryproxy.common.Configurable;
import org.shirdrn.queryproxy.common.ConfiguredQueryService;
import org.shirdrn.queryproxy.thrift.protocol.QueryFailureException;
import org.shirdrn.queryproxy.thrift.protocol.QueryParams;
import org.shirdrn.queryproxy.thrift.protocol.QueryResult;
import org.shirdrn.queryproxy.utils.PropertiesConfig;
import org.shirdrn.queryproxy.utils.ResultUtils;

public class SQLQueryService extends ConfiguredQueryService {

     private static final Log LOG = LogFactory.getLog(SQLQueryService.class);
     private static String JDBC_PROPERTIES = "jdbc.properties";
     Configurable jdbcConf;
     private String jdbcUrl;
     private String user;
     private String password;
     Connection connection;

     public SQLQueryService(Configurable context) {
          super(context);
          jdbcConf = new PropertiesConfig(JDBC_PROPERTIES);
          String driverClass = jdbcConf.get("jdbc.driverClass");
          try {
               Class.forName(driverClass);
               jdbcUrl = jdbcConf.get("jdbc.jdbcUrl");
               user = jdbcConf.get("jdbc.user");
               password = jdbcConf.get("jdbc.password");
          } catch (ClassNotFoundException e) {
               throw new RuntimeException(e);
          } finally {
               LOG.info("JDBC: driver=" + driverClass + ", url=" + jdbcUrl + ", user=" + user + ", password=******");
          }
     }

     @Override
     public QueryResult query(QueryParams params) throws QueryFailureException, TException {
          QueryResult result = new QueryResult();
          if(!params.getParamList().isEmpty()) {
               // get SQL statement
               String sql = params.getParamList().remove(0);
               Connection conn = getConnection();
               Statement stmt = null;
               ResultSet rs = null;
               try {
                    stmt = conn.createStatement();
                    rs = stmt.executeQuery(sql);
                    result.setResults(ResultUtils.getJSONResults(rs, params.getParamList()));
               } catch (SQLException e) {
                    throw new QueryFailureException(e.toString());
               }
          }
          return result;
     }

     private synchronized final Connection getConnection() {
          try {
               if(connection == null || connection.isClosed()) {
                    if(user != null) {
                         connection = DriverManager.getConnection(jdbcUrl, user, password);
                    } else {
                         connection = DriverManager.getConnection(jdbcUrl);
                    }
               }
          } catch (SQLException e) {
               e.printStackTrace();
          }
          return connection;
     }

     @Override
     public void close() throws IOException {
          if(connection != null) {
               try {
                    connection.close();
               } catch (SQLException e) {
                    throw new IOException(e);
               }
          }
     }
}
