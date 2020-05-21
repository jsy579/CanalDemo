package com.jsy.canaltest.canal.client;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.annotation.Resource;
import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang.StringUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.Pair;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.CanalEntry.TransactionBegin;
import com.alibaba.otter.canal.protocol.CanalEntry.TransactionEnd;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

@Component
@PropertySource("classpath:canal.properties")
@ConfigurationProperties(prefix = "canal")
public class CanalClient {
	private String ip;
	private int port;
	private String destination;
	private String username;
	private String password;
	
	
	private Queue<String> SQL_QUEUE = new ConcurrentLinkedQueue<>();
	private volatile boolean running = false;
	private Thread thread = null;

	@Resource
	private DataSource dataSource;
	
	public void process() {
		thread = new Thread(() -> {
			run();
		});
		this.running = true;
		thread.start();
	}

	public void run() {
		System.out.println("run....");
		CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(ip, port), destination,
				username, password);
		int batchSize = 5 * 1024;
		connector.connect();
		connector.subscribe(".*\\..*");
		connector.rollback();
		try {
			while (running) {
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

				if (SQL_QUEUE.size() > 0) {
					executeQueueSql();
				}

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

	private void executeQueueSql() {
		while(null != SQL_QUEUE.peek()) {
			String sql = SQL_QUEUE.poll();
			System.out.println("[sql]-----> " + sql);
			execute(sql);
		}
	}

	private void execute(String sql) {
		try {
			QueryRunner qr = new QueryRunner(dataSource);
			int row = qr.update(sql);
			System.out.println("update: " + row);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	protected void printEntry(List<Entry> entrys) {
		for (Entry entry : entrys) {
			String tableName = entry.getHeader().getTableName();

			if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN
					|| entry.getEntryType() == EntryType.TRANSACTIONEND) {
				if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN) {
					TransactionBegin begin = null;
					try {
						begin = TransactionBegin.parseFrom(entry.getStoreValue());
					} catch (InvalidProtocolBufferException e) {
						throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
					}
					// 打印事务头信息，执行的线程id，事务耗时
					printXAInfo(begin.getPropsList());
				} else if (entry.getEntryType() == EntryType.TRANSACTIONEND) {
					TransactionEnd end = null;
					try {
						end = TransactionEnd.parseFrom(entry.getStoreValue());
					} catch (InvalidProtocolBufferException e) {
						throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
					}
					// 打印事务提交信息，事务id
					printXAInfo(end.getPropsList());
				}

				continue;
			}

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
                        saveDeleteSql(rowData, tableName);
					} else if (eventType == EventType.INSERT) {
						saveInsertSql(rowData, tableName);
					} else {
						saveUpdateSql(rowData, tableName);
					}
				}
			}
		}
	}
	
	protected void saveUpdateSql(RowData rowData, String tableName) {
		List<Column> columns = rowData.getAfterColumnsList();
		
		StringBuilder mainSql = new StringBuilder();
		mainSql.append(" update ").append(tableName).append(" set ");

		for (int i = 0; i < columns.size(); i++) {
			try {
				Column column = columns.get(i);
				mainSql.append(column.getName());
				mainSql.append("=");
                if (StringUtils.containsIgnoreCase(column.getMysqlType(), "BLOB")
                    || StringUtils.containsIgnoreCase(column.getMysqlType(), "BINARY")) {
                    // get value bytes
                	mainSql.append("'" + new String(column.getValue().getBytes("ISO-8859-1"), "UTF-8") + "'");
                } else {
                	mainSql.append("'" + column.getValue() + "'");
                }
            } catch (UnsupportedEncodingException e) {
            	e.printStackTrace();
            }
			
			if (i != columns.size() - 1) {
				mainSql.append(" , ");
			}
		}
		mainSql.append(" where ");
		
		List<Column> oldColumnList = rowData.getBeforeColumnsList();
		for(Column column : oldColumnList) {
			if(column.getIsKey()) {
				mainSql.append(column.getName() + "=" + column.getValue());
				break;
			}
		}
		SQL_QUEUE.add(mainSql.toString());
	}
	
	protected void saveDeleteSql(RowData rowData, String tableName) {
		List<Column> columnList = rowData.getBeforeColumnsList();
		
		StringBuilder mainSql = new StringBuilder();
		mainSql.append(" delete from ").append(tableName).append(" where ");
		for(Column column : columnList) {
			if(column.getIsKey()) {
				mainSql.append(column.getName() + "=" + column.getValue());
				break;
			}
		}
		SQL_QUEUE.add(mainSql.toString());
	}

	protected void saveInsertSql(RowData rowData, String tableName) {
		List<Column> columns = rowData.getAfterColumnsList();
		
		StringBuilder mainSql = new StringBuilder();
		mainSql.append(" insert into ").append(tableName).append(" ( ");

		StringBuilder nameSql = new StringBuilder();
		StringBuilder valueSql = new StringBuilder();

		for (int i = 0; i < columns.size(); i++) {
			
			try {
                if (StringUtils.containsIgnoreCase(columns.get(i).getMysqlType(), "BLOB")
                    || StringUtils.containsIgnoreCase(columns.get(i).getMysqlType(), "BINARY")) {
                    // get value bytes
        			valueSql.append("'" + new String(columns.get(i).getValue().getBytes("ISO-8859-1"), "UTF-8") + "'");
                } else {
                	valueSql.append("'" + columns.get(i).getValue() + "'");
                }
            } catch (UnsupportedEncodingException e) {
            	e.printStackTrace();
            }
			
			nameSql.append(columns.get(i).getName());
			if (i != columns.size() - 1) {
				nameSql.append(" , ");
				valueSql.append(" , ");
			}
		}
		nameSql.append(" ) values( ");
		valueSql.append(" ); ");
		mainSql.append(nameSql).append(valueSql);
		SQL_QUEUE.add(mainSql.toString());
	}

	protected void printXAInfo(List<Pair> pairs) {
		if (pairs == null) {
			return;
		}

		String xaType = null;
		String xaXid = null;
		for (Pair pair : pairs) {
			String key = pair.getKey();
			if (StringUtils.endsWithIgnoreCase(key, "XA_TYPE")) {
				xaType = pair.getValue();
			} else if (StringUtils.endsWithIgnoreCase(key, "XA_XID")) {
				xaXid = pair.getValue();
			}
		}

		if (xaType != null && xaXid != null) {
			System.out.println(" ------> " + xaType + " " + xaXid);
		}
	}
	
	/**
	 * Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    clientTest.stop();
                } catch (Throwable e) {

                }
	 */
	public void stop() {
		if(!running) {
			return;
		}
		running = false;
		if(thread != null) {
			try {
				thread.join();
			}catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		CanalClient canalClient = new CanalClient();
		canalClient.process();
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getDestination() {
		return destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
	
}
