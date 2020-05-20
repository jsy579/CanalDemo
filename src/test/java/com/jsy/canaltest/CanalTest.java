package com.jsy.canaltest;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;

public class CanalTest {
	private final String SEP = SystemUtils.LINE_SEPARATOR;
	
	public void run() {
		String ip = "192.168.0.106";
		String destination = "example";
		CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(ip, 11111), destination,
				"canal", "canal");
		int batchSize = 5 * 1024;
		connector.connect();
		connector.subscribe(".*\\..*");
		connector.rollback();
		try {
			while (true) {
				Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
				long batchId = message.getId();
				int size = message.getEntries().size();
				if (batchId == -1 || size == 0) {
					try {
						Thread.sleep(1000);
						continue;
					} catch (InterruptedException e) {
						// TODO
					}
				} else {
//                    printSummary(message, batchId, size);
					printEntry(message.getEntries());
				}

				if (batchId != -1) {
					connector.ack(batchId); // 提交确认
					// connector.rollback(batchId); // 处理失败, 回滚数据
				}

//				if (SQL_QUEUE.size() > 0) {
//					executeQueueSql();
//				}

			}
		} catch (Exception e) {
			try {
				Thread.sleep(1000L);
			} catch (InterruptedException e1) {
				// ignore
			}
		} finally {
			connector.disconnect();
		}
	}
	
	protected void printEntry(List<Entry> entrys) {
        for (Entry entry : entrys) {

            if (entry.getEntryType() == EntryType.ROWDATA) {
                RowChange rowChage = null;
                try {
                    rowChage = RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }

                EventType eventType = rowChage.getEventType();


                if (eventType == EventType.QUERY || rowChage.getIsDdl()) {
                    continue;
                }

                for (RowData rowData : rowChage.getRowDatasList()) {
                    if (eventType == EventType.DELETE) {
                        printColumn(rowData.getBeforeColumnsList());
                    } else if (eventType == EventType.INSERT) {
                        printColumn(rowData.getAfterColumnsList());
                    } else {
                        printColumn(rowData.getAfterColumnsList());
                    }
                }
            }
        }
    }

    protected void printColumn(List<Column> columns) {
    	StringBuilder builder = new StringBuilder();
        for (Column column : columns) {
            try {
                if (StringUtils.containsIgnoreCase(column.getMysqlType(), "BLOB")
                    || StringUtils.containsIgnoreCase(column.getMysqlType(), "BINARY")) {
                    // get value bytes
                    builder.append(column.getName() + " : "
                                   + new String(column.getValue().getBytes("ISO-8859-1"), "UTF-8"));
                } else {
                    builder.append(column.getName() + " : " + column.getValue());
                }
            } catch (UnsupportedEncodingException e) {
            }
            builder.append("    type=" + column.getMysqlType());
            if (column.getUpdated()) {
                builder.append("    update=" + column.getUpdated());
            }
            builder.append(SEP);
        }
        System.out.println(builder.toString());
    }
	
    public static void main(String[] args) {
		CanalTest a = new CanalTest();
		a.run();
	}
    
}
