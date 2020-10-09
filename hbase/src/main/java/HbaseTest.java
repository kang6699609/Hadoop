import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @author yuanxiaokang
 * data 2020/9/28
 * 描述：
 */
public class HbaseTest {
    static Configuration configuration =null;
    static {
        configuration = HBaseConfiguration.create();
        //创建配置文件对象，并指定 zookeeper 的连接地址
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "hadoop1,hadoop4,hadoop5");
    }
    @Test
     void createTable() throws IOException {
        //创建配置文件对象，并指定 zookeeper 的连接地址
        //集群配置↓
       // configuration.set("hbase.master", "hadoop5:60000");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        //通过 HTableDescriptor 来实现我们表的参数设置，包括表名，列族等等
        HTableDescriptor hTableDescriptor = new
                HTableDescriptor(TableName.valueOf("kk.user"));
        hTableDescriptor.addFamily(new HColumnDescriptor("data"));
        admin.createTable(hTableDescriptor);

        //hTableDescriptor.addFamily(new HColumnDescriptor("other"));
        //添加列族other
        //创建表
        boolean myuser = admin.tableExists(TableName.valueOf("kk.user"));
        if (!myuser) {
            admin.createTable(hTableDescriptor);
        }
        //关闭客户端连接
        admin.close();
    }

    @Test
    public void searchData() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop4:2181,hadoop5:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table user = connection.getTable(TableName.valueOf("cgc.cgc_user_basic"));
        Get get = new Get(Bytes.toBytes("321000000091005280694055resUP"));
        Result result = user.get(get);
        Cell[] cells = result.rawCells();
        //获取所有的列名称以及列的值
        for (Cell cell : cells) {
        //注意，如果列属性是 int 类型，那么这里就不会显示
            System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
            System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }
        user.close();
    }
}
