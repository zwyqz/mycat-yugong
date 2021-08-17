package com.taobao.yugong.translator.celoan;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.translator.SourceBackTableDataTranslator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import javax.sql.DataSource;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Objects;

/**
 * 测试类 回调测试
 *
 * @author wyzhang
 * @date 2020/8/18 16:58
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class ClientCompanyToEnterpriseOperatorDataTranslator extends SourceBackTableDataTranslator {

  /**
   * *
   *
   * <p>委托书模板 0 营业执照 1 法人身份证 2 经办人身份证 3 委托授权书 4
   *
   * @param targetDs
   * @param value
   * @return
   */
  public void replaceItemValue(DataSource targetDs, ColumnValue value, Integer fileType) {
    if (value == null || value.getValue() == null || "".equals(value.getValue())) {
      return;
    }
    JdbcTemplate jdbcTemplate = new JdbcTemplate(targetDs);

    String sql = "insert into ec_enterprise_file ( file_type, file_url, create_time) value(?,?,?)";
    // 获取插入数据的自增主键
    KeyHolder holder = new GeneratedKeyHolder();
    jdbcTemplate.update(
        connection -> {
          PreparedStatement ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
          ps.setInt(1, fileType);
          ps.setString(2, value.getValue().toString());
          ps.setDate(3, new Date(System.currentTimeMillis()));
          return ps;
        },
        holder);

    value.setValue(Objects.requireNonNull(holder.getKey()).longValue());
  }

  @Override
  public boolean translator(DataSource sourceDs, DataSource targetDs, Record record) {
    /** 法人身份证头像文件Id */
    ColumnValue urlaFileId = record.getColumnByName("urla_file_id", true);
    replaceItemValue(targetDs, urlaFileId, 2);

    /** 法人身份证国徽文件Id */
    ColumnValue urlb_file_id = record.getColumnByName("urlb_file_id", true);
    replaceItemValue(targetDs, urlb_file_id, 2);

    return super.translator(record);
}
}
