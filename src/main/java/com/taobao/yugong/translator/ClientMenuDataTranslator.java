package com.taobao.yugong.translator;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;

import javax.sql.DataSource;
import java.sql.*;

/**
 * 测试类 回调测试
 *
 * @author wyzhang
 * @date 2020/8/18 16:58
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class ClientMenuDataTranslator extends BackTableDataTranslator {

    @Override
    public boolean translator(DataSource sourceDs, Record record) {

        try {
            Connection con = sourceDs.getConnection();
            PreparedStatement pstmt = null;
            pstmt = con.prepareStatement("select name from t_base_cpc where code = ?");
            pstmt.setObject(1, record.getColumnByName("city_code").getValue());

            ResultSet rs = pstmt.executeQuery();
            String name = "";
            while(rs.next()) {
                name = rs.getString("name");
            }

            ColumnMeta columnMeta = new ColumnMeta("area_name", Types.VARCHAR);
            ColumnValue val = new ColumnValue(columnMeta, name);
            record.addColumn(val);
        } catch (SQLException e) {
            throw new RuntimeException("sql 错误" + e.getMessage(), e);

        }
        return true;
    }

}
