package com.taobao.yugong.translator;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

import javax.sql.DataSource;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 测试类 回调测试
 *
 * @author wyzhang
 * @date 2020/8/18 16:58
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class DictDataTranslator extends SourceBackTableDataTranslator {
  public static final long CURRENT_TIME = 1599719869901L;
  public static final String PASSWORD = "123456";

  private String dictCenterDatabase;

  public void setDictCenterDatabase(String dictCenterDatabase) {
    this.dictCenterDatabase = dictCenterDatabase;
  }

//  public String setCahce(DataSource ds, String sql, Map<String, String> map ) {
//    JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
//    return (String)
//        jdbcTemplate.query(
//            sql,
//            new ResultSetExtractor() {
//              @Override
//              public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
//                if (rs.next()) {
//                  map.put(rs.getString(1), rs.getString(2));
//                  return rs.getString(1);
//                }
//                return null;
//              }
//            });
//  }
  protected final static  Logger logger = LoggerFactory.getLogger(DictDataTranslator.class);

  public void setCahce(DataSource ds, String sql, Map<String, String> map) {
    Connection con = null;
    try {
      con = ds.getConnection();
      PreparedStatement pstmt = null;
      pstmt = con.prepareStatement(sql);
      ResultSet rs = pstmt.executeQuery();
      while(rs.next()) {
        try {
          if(rs.getString(2) == null) {
            continue;
          }
          map.put(rs.getString(1), rs.getString(2));
        }catch (Exception e) {
          logger.error("字典错误",e);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  public String setCache(DataSource sourceDs, DataSource targetDs) {

    setCahce(sourceDs, "select code, name from  t_base_cpc ", SOURCE_MAP);
    setCahce(targetDs, "select dict_name, dict_value FROM " + dictCenterDatabase + ".sys_dict  ", TARGET_MAP);

    return "";
  }

  public static AtomicBoolean init = new AtomicBoolean(false);

  public static Map<String, String> SOURCE_MAP = new ConcurrentHashMap<>();
  public static Map<String, String> TARGET_MAP = new ConcurrentHashMap<>();
  public static ReentrantLock lock = new ReentrantLock();
  /***
   * 查找省市名称
   * @param key
   * @return
   */
  public static String getSourceName(String key) {

    if(init.get() == false) {
       throw new RuntimeException("请配置SourceBackTableDataTranslator进行省市区缓存的配置");
    }

    if(null == key) {
      return null;
    }
    return SOURCE_MAP.get(key);
  }
  /***
   * 查找目标库省市编码
   * @param name
   * @return
   */
  public static String getTargetKey(String name) {
    if(init.get() == false) {
      throw new RuntimeException("请配置SourceBackTableDataTranslator进行省市区缓存的配置");
    }

    if(null == name) {
      return null;
    }

    return TARGET_MAP.get(name);
  }

  @Override
  public boolean translator(DataSource sourceDs, DataSource targetDs, Record record) {
    if (init.get()) {
      return super.translator(record);
    }
    lock.lock();
    try {
      if (init.get()) {
        return super.translator(record);
      }
      setCache(sourceDs, targetDs);
      init.compareAndSet(false, true);
    } finally{
      lock.unlock();
    }


    return super.translator(record);
  }
}
