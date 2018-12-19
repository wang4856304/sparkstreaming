import com.wj.SparkStreamApp;
import com.wj.dao.BaseDao;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author wangJun
 * @Description //TODO
 * @Date ${date} ${time}
 **/

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = SparkStreamApp.class)
@WebAppConfiguration
public class HBasePhoenixTest {

    @Autowired
    private BaseDao baseDao;

    @Test
    public void test() {
        /*Statement stmt;
        Connection con;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            con = DriverManager.getConnection("jdbc:phoenix:192.168.209.129:2181" );
            stmt = con.createStatement();
            stmt.execute("upsert into student(IDCARDNUM, \"column1\".\"Name\", \"column2\".\"Age\", \"column1\".\"identy_num\") values(102,'wangjun', 30, '1455666')");
            stmt.close();
            con.commit();
            con.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }*/
        List<Map<String, Object>> map = baseDao.select(new HashMap<>());
        System.out.println(map);
    }
}
