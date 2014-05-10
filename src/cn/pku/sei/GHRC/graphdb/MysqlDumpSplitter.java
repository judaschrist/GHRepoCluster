package cn.pku.sei.GHRC.graphdb;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


public class MysqlDumpSplitter {

	private File sqlFile = null;
	private BufferedReader sqlReader = null;
	private BufferedWriter sqlWriter = null;
	
	public static void main(String[] args) {
		MysqlDumpSplitter splitter = new MysqlDumpSplitter("d:/ghtorrent.dump");
		try {
			splitter.split();
//			splitter.test();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void split(String tableName) throws IOException {
//		sqlWriter = new BufferedWriter(new FileWriter(new File("d:/test.sql")));
		
		String line = null;
		while ((line = sqlReader.readLine()) != null) {
			if (line.startsWith("DROP TABLE IF EXISTS")) {
				if (sqlWriter != null) {
					sqlWriter.close();
				}
				int i = line.indexOf("`");
				int j = line.indexOf("`", i+1);
				String tmpTableName = line.substring(i + 1, j);
				if (tmpTableName.equals(tableName)) {
					sqlWriter = new BufferedWriter(new FileWriter(new File("d:/" + tableName + ".sql")));
				}
			}
			if (sqlWriter != null) {
				sqlWriter.write(line);
				sqlWriter.newLine();
			}
		}
		
		sqlReader.close();
		sqlWriter.close();
	}
	
	public void split() throws IOException {
//		sqlWriter = new BufferedWriter(new FileWriter(new File("d:/test.sql")));
		
		String line = null;
		String tableName = null;
//		sqlWriter = new BufferedWriter(new FileWriter(new File("d:/" + tableName + ".sql")));
		int linecount = 0; 
		while ((line = sqlReader.readLine()) != null) {
			
			if (line.startsWith("DROP TABLE IF EXISTS")) {
				if (sqlWriter != null) {
					sqlWriter.close();
					System.out.println("------------Done Writing Table " + tableName + "-------------------");
				}
				int i = line.indexOf("`");
				int j = line.indexOf("`", i+1);
				tableName = line.substring(i + 1, j);
				sqlWriter = new BufferedWriter(new FileWriter(new File("d:/" + tableName + ".sql")));
				System.out.println("------------Start Writing Table " + tableName + "-------------------");
			}
			if (sqlWriter != null && 
					line.startsWith("INSERT")) {
				System.out.println(tableName + "\t" + linecount);
				sqlWriter.write(line);
				sqlWriter.newLine();
			}
			linecount++; 
		}
		
		sqlReader.close();
		sqlWriter.close();
	}
	
	public void test() throws IOException {
//		sqlWriter = new BufferedWriter(new FileWriter(new File("d:/test.sql")));
		
		String line = null;
		String tableName = null;
		int linecount = 0; 
		while ((line = sqlReader.readLine()) != null && linecount < 40) {
			System.out.println(line);
			linecount++; 
		}
		
		sqlReader.close();
//		sqlWriter.close();
	}

	public MysqlDumpSplitter(String sqlFilePath) {
		try {
			this.sqlFile = new File(sqlFilePath);
			this.sqlReader = new BufferedReader(new FileReader(sqlFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
}
