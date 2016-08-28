package HBaseDataImporter;

import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Button;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Random;

import javax.swing.JFileChooser;
import javax.swing.filechooser.FileNameExtensionFilter;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.swt.SWT;
import HBaseOperate.HBaseOperate;
import HBaseOperate.data_importer;
import OracleConnector.Oracle_Connector;
import Tools.qualifier_generator;
import Tools.rowkey_generator;

import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.TabFolder;
import org.eclipse.swt.widgets.TabItem;
import org.eclipse.ui.forms.widgets.FormToolkit;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.wb.swt.SWTResourceManager;

import CsvFileImporter.CsvDataImporter;

public class HBaseDataImporter {

	protected Shell shell;
	private final FormToolkit formToolkit = new FormToolkit(Display.getDefault());
	private Text text_file_names;
	private Text text_file_data;
	private Text text_encode_data;
	private Text text_table_name;
	private Text text_family_name;
	private Text text_operation_status;
	private Text text_url;
	private Text text_user_name;
	private Text text_pwd;
	private Text text_oracle_data;
	private Text text_hbase_data;
	
	private final String[] table_operation_selection={"Create","Drop"};
	private final String[] oracle_dbs={"orcl"};
	private final String[] orcl_tables={"ysp"};
	private Text text_file_tb_name;
	private Text text_path_name;
	private Text text_file_columnFamily;

	/**
	 * Launch the application.
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			HBaseDataImporter window = new HBaseDataImporter();
			window.open();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Open the window.
	 */
	public void open() {
		Display display = Display.getDefault();
		createContents();
		shell.open();
		shell.layout();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}
	}

	/**
	 * Create contents of the window.
	 */
	protected void createContents() {
		shell = new Shell();
		shell.setSize(800, 600);
		shell.setText("HBase Data Importer");
		shell.setLayout(null);
		
		Composite composite = new Composite(shell, SWT.NONE);
		composite.setBounds(0, 0, 800, 600);
		
		TabFolder tabFolder = new TabFolder(composite, SWT.NONE);
		tabFolder.setBounds(0, 0, 800, 600);
		
		TabItem file_data_import = new TabItem(tabFolder, SWT.NONE);
		file_data_import.setText("\u6587\u4EF6\u6570\u636E\u5BFC\u5165");
		
		Composite file_importer_composite = new Composite(tabFolder, SWT.NONE);
		file_importer_composite.setBounds(0, 0, 800, 600);
		file_data_import.setControl(file_importer_composite);
		formToolkit.paintBordersFor(file_importer_composite);
		
		text_file_names = new Text(file_importer_composite, SWT.BORDER | SWT.READ_ONLY | SWT.MULTI);
		text_file_names.setFont(SWTResourceManager.getFont("Î¢ÈíÑÅºÚ", 11, SWT.NORMAL));
		text_file_names.setEditable(false);
		text_file_names.setBounds(20, 39, 401, 41);
		formToolkit.adapt(text_file_names, true, true);
		
		Button choose_file_btn = new Button(file_importer_composite, SWT.NONE);
		
		choose_file_btn.setBounds(440, 39, 125, 41);
		formToolkit.adapt(choose_file_btn, true, true);
		choose_file_btn.setText("\u9009\u62E9\u6587\u4EF6...");
		
		Button file_import_confirm_btn = new Button(file_importer_composite, SWT.NONE);
		file_import_confirm_btn.setBounds(440, 172, 125, 41);
		formToolkit.adapt(file_import_confirm_btn, true, true);
		file_import_confirm_btn.setText("\u786E\u8BA4\u5BFC\u5165");
		
		text_file_data = new Text(file_importer_composite, SWT.BORDER | SWT.READ_ONLY | SWT.V_SCROLL | SWT.MULTI);
		text_file_data.setBounds(20, 259, 350, 236);
		formToolkit.adapt(text_file_data, true, true);
		
		text_encode_data = new Text(file_importer_composite, SWT.BORDER | SWT.READ_ONLY | SWT.V_SCROLL | SWT.MULTI);
		text_encode_data.setBounds(398, 259, 350, 236);
		formToolkit.adapt(text_encode_data, true, true);
		
		Label file_names = new Label(file_importer_composite, SWT.NONE);
		file_names.setBounds(20, 10, 85, 17);
		formToolkit.adapt(file_names, true, true);
		file_names.setText("\u6570\u636E\u8868\u8DEF\u5F84\uFF1A");
		
		Label data_in_file = new Label(file_importer_composite, SWT.NONE);
		data_in_file.setBounds(20, 236, 99, 17);
		formToolkit.adapt(data_in_file, true, true);
		data_in_file.setText("\u6587\u4EF6\u4E2D\u6570\u636E\uFF1A");
		
		Label file_data_encoded = new Label(file_importer_composite, SWT.NONE);
		file_data_encoded.setBounds(398, 236, 136, 17);
		formToolkit.adapt(file_data_encoded, true, true);
		file_data_encoded.setText("\u7F16\u7801\u6570\u636E\uFF1A");
		
		Label table_name = new Label(file_importer_composite, SWT.NONE);
		table_name.setBounds(20, 156, 85, 17);
		formToolkit.adapt(table_name, true, true);
		table_name.setText("\u5BFC\u5165\u6570\u636E\u8868\uFF1A");
		
		text_file_tb_name = new Text(file_importer_composite, SWT.BORDER);
		text_file_tb_name.setFont(SWTResourceManager.getFont("Î¢ÈíÑÅºÚ", 11, SWT.NORMAL));
		text_file_tb_name.setBounds(20, 178, 195, 35);
		formToolkit.adapt(text_file_tb_name, true, true);
		
		Label reflect_file_path = new Label(file_importer_composite, SWT.NONE);
		reflect_file_path.setBounds(20, 86, 85, 17);
		formToolkit.adapt(reflect_file_path, true, true);
		reflect_file_path.setText("\u7F16\u7801\u8868\u8DEF\u5F84\uFF1A");
		
		text_path_name = new Text(file_importer_composite, SWT.BORDER);
		text_path_name.setFont(SWTResourceManager.getFont("Î¢ÈíÑÅºÚ", 11, SWT.NORMAL));
		text_path_name.setBounds(20, 109, 401, 41);
		formToolkit.adapt(text_path_name, true, true);
		
		Button choose_path_btn = new Button(file_importer_composite, SWT.NONE);
		choose_path_btn.setBounds(438, 109, 125, 41);
		formToolkit.adapt(choose_path_btn, true, true);
		choose_path_btn.setText("\u9009\u62E9\u8DEF\u5F84...");
		
		Label file_columnFamily = new Label(file_importer_composite, SWT.NONE);
		file_columnFamily.setBounds(227, 156, 48, 17);
		formToolkit.adapt(file_columnFamily, true, true);
		file_columnFamily.setText("\u5217\u7C07\u540D\uFF1A");
		
		text_file_columnFamily = new Text(file_importer_composite, SWT.BORDER);
		text_file_columnFamily.setBounds(232, 178, 189, 35);
		formToolkit.adapt(text_file_columnFamily, true, true);
		
		TabItem oracle_data_import = new TabItem(tabFolder, SWT.NONE);
		oracle_data_import.setText("Oracle\u6570\u636E\u5BFC\u5165");
		
		Composite oracle_importer_composite = new Composite(tabFolder, SWT.NONE);
		oracle_data_import.setControl(oracle_importer_composite);
		formToolkit.paintBordersFor(oracle_importer_composite);
		
		Label url = new Label(oracle_importer_composite, SWT.NONE);
		url.setBounds(10, 10, 100, 30);
		formToolkit.adapt(url, true, true);
		url.setText("\u670D\u52A1\u5668IP:");
		
		Label user_name = new Label(oracle_importer_composite, SWT.NONE);
		user_name.setBounds(10, 50, 100, 30);
		formToolkit.adapt(user_name, true, true);
		user_name.setText("\u7528\u6237\u540D\uFF1A");
		
		Label passwd = new Label(oracle_importer_composite, SWT.NONE);
		passwd.setBounds(10, 90, 100, 30);
		formToolkit.adapt(passwd, true, true);
		passwd.setText("\u5BC6\u7801\uFF1A");
		
		text_url = new Text(oracle_importer_composite, SWT.BORDER);
		text_url.setBounds(116, 10, 300, 30);
		formToolkit.adapt(text_url, true, true);
		
		text_user_name = new Text(oracle_importer_composite, SWT.BORDER);
		text_user_name.setBounds(116, 50, 300, 30);
		formToolkit.adapt(text_user_name, true, true);
		
		text_pwd = new Text(oracle_importer_composite, SWT.BORDER | SWT.PASSWORD);
		text_pwd.setBounds(116, 90, 300, 30);
		formToolkit.adapt(text_pwd, true, true);
		
		final Combo oracle_tb_selector = new Combo(oracle_importer_composite, SWT.NONE);
		oracle_tb_selector.setBounds(537, 46, 196, 30);
		formToolkit.adapt(oracle_tb_selector);
		formToolkit.paintBordersFor(oracle_tb_selector);
		oracle_tb_selector.setText("select table..");
		oracle_tb_selector.setItems(orcl_tables);
		
		Button oracle_import_confirm = new Button(oracle_importer_composite, SWT.NONE);
		oracle_import_confirm.setBounds(455, 90, 106, 29);
		formToolkit.adapt(oracle_import_confirm, true, true);
		oracle_import_confirm.setText("\u786E\u8BA4");
		
		text_oracle_data = new Text(oracle_importer_composite, SWT.BORDER | SWT.V_SCROLL | SWT.MULTI);
		text_oracle_data.setEditable(false);
		text_oracle_data.setBounds(10, 195, 350, 280);
		formToolkit.adapt(text_oracle_data, true, true);
		
		text_hbase_data = new Text(oracle_importer_composite, SWT.BORDER | SWT.V_SCROLL | SWT.MULTI);
		text_hbase_data.setBounds(365, 195, 350, 280);
		formToolkit.adapt(text_hbase_data, true, true);
		
		Label oracle_db_name = new Label(oracle_importer_composite, SWT.NONE);
		oracle_db_name.setBounds(445, 10, 85, 30);
		formToolkit.adapt(oracle_db_name, true, true);
		oracle_db_name.setText("\u6570\u636E\u5E93\uFF1A");
		
		final Combo oracle_db_selector = new Combo(oracle_importer_composite, SWT.NONE);
		oracle_db_selector.setBounds(536, 10, 197, 30);
		formToolkit.adapt(oracle_db_selector);
		formToolkit.paintBordersFor(oracle_db_selector);
		oracle_db_selector.setText("select db..");
		oracle_db_selector.setItems(oracle_dbs);
		
		Label lb_oracle_table_name = new Label(oracle_importer_composite, SWT.NONE);
		lb_oracle_table_name.setBounds(445, 50, 91, 30);
		formToolkit.adapt(lb_oracle_table_name, true, true);
		lb_oracle_table_name.setText("\u6570\u636E\u8868\uFF1A");
		
		Label lbl_oracle_table_data = new Label(oracle_importer_composite, SWT.NONE);
		lbl_oracle_table_data.setBounds(10, 161, 162, 17);
		formToolkit.adapt(lbl_oracle_table_data, true, true);
		lbl_oracle_table_data.setText("Oracle\u4E2D\u6570\u636E\uFF1A");
		
		Label lbl_encoded_oracle_data = new Label(oracle_importer_composite, SWT.NONE);
		lbl_encoded_oracle_data.setBounds(365, 161, 123, 28);
		formToolkit.adapt(lbl_encoded_oracle_data, true, true);
		lbl_encoded_oracle_data.setText("\u7F16\u7801\u6570\u636E\uFF1A");
		
		TabItem table_operate = new TabItem(tabFolder, SWT.NONE);
		table_operate.setText("HBase\u6570\u636E\u8868\u64CD\u4F5C");
		
		Composite hbase_operate_composite = new Composite(tabFolder, SWT.NONE);
		table_operate.setControl(hbase_operate_composite);
		formToolkit.paintBordersFor(hbase_operate_composite);
		
		text_table_name = new Text(hbase_operate_composite, SWT.BORDER);
		text_table_name.setBounds(140, 10, 330, 30);
		formToolkit.adapt(text_table_name, true, true);
		
		Label TableName = new Label(hbase_operate_composite, SWT.NONE);
		TableName.setBounds(10, 10, 124, 30);
		formToolkit.adapt(TableName, true, true);
		TableName.setText("\u6570\u636E\u8868\u540D\uFF1A");
		
		text_family_name = new Text(hbase_operate_composite, SWT.BORDER);
		text_family_name.setBounds(140, 50, 330, 30);
		formToolkit.adapt(text_family_name, true, true);
		
		Label FamilyName = new Label(hbase_operate_composite, SWT.NONE);
		FamilyName.setBounds(10, 50, 124, 30);
		formToolkit.adapt(FamilyName, true, true);
		FamilyName.setText("\u5217\u65CF\u540D\uFF1A");
		
		text_operation_status = new Text(hbase_operate_composite, SWT.BORDER | SWT.READ_ONLY | SWT.MULTI);
		text_operation_status.setBounds(10, 184, 600, 305);
		formToolkit.adapt(text_operation_status, true, true);
		
		Button Hbase_Confirm = new Button(hbase_operate_composite, SWT.NONE);
		
		Hbase_Confirm.setBounds(346, 103, 124, 29);
		formToolkit.adapt(Hbase_Confirm, true, true);
		Hbase_Confirm.setText("\u786E\u8BA4");
		
		final Combo operation_selector = new Combo(hbase_operate_composite, SWT.NONE);
		operation_selector.setItems(table_operation_selection);
		operation_selector.setBounds(140, 106, 166, 25);
		formToolkit.adapt(operation_selector);
		formToolkit.paintBordersFor(operation_selector);
		
		Label lbl_operation = new Label(hbase_operate_composite, SWT.NONE);
		lbl_operation.setBounds(10, 106, 93, 30);
		formToolkit.adapt(lbl_operation, true, true);
		lbl_operation.setText("\u64CD\u4F5C\uFF1A");
		
		Label lbl_status = new Label(hbase_operate_composite, SWT.NONE);
		lbl_status.setBounds(10, 148, 161, 30);
		formToolkit.adapt(lbl_status, true, true);
		lbl_status.setText("\u64CD\u4F5C\u72B6\u6001\uFF1A");
		
		Hbase_Confirm.addMouseListener(new MouseAdapter(){
			@Override
			public void mouseDown(MouseEvent e) {
				String tb_name=text_table_name.getText().trim();
				String fm_name=text_family_name.getText().trim();
				String operate=operation_selector.getText();
				if(operate.equals("Create"))
				{
					try {
						if((!tb_name.equals(""))&&(!fm_name.equals("")))
						{
							String result=HBaseOperate.creat(tb_name, fm_name);
							text_operation_status.setText(result);
						}
						else
						{
							text_operation_status.setText("Incomplete information! Table name or Family name is empty!");
						}
						
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
				
				if(operate.equals("Drop"))
				{
					if(!tb_name.equals(""))
					{
						String result;
						try {
							result = HBaseOperate.drop(tb_name);
							text_operation_status.setText(result);
						} catch (MasterNotRunningException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						} catch (ZooKeeperConnectionException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
					}
					else
					{
						text_operation_status.setText("Table Name Not Right!");
					}
				}
			}
		});
		
		
		oracle_import_confirm.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				String url=text_url.getText().trim();
				String db_name=oracle_db_selector.getText().trim();
				String tb_name=oracle_tb_selector.getText().trim();
				String user=text_user_name.getText().trim();
				String pwd=text_pwd.getText().trim();
				
				//check the integrity of the parameters
				content_detector cd=new content_detector();
				String result=cd.oracle_parameters_detect(url, db_name, tb_name, user, pwd);
				
				//import data
				if(result.equals("0"))
				{
					//read all data of the table into memory
					Oracle_Connector oc=new Oracle_Connector(url,db_name,tb_name,user,pwd);
					byte[] columnFamily=Bytes.toBytes("a");
					
					//define a data importer
					data_importer di=new data_importer();
					try {
						di.creat(tb_name, columnFamily);
					} catch (Exception e2) {
						// TODO Auto-generated catch block
						e2.printStackTrace();
					}
					try {
						//get all needed data from oracle database
						HashMap<Integer,HashMap<String,String>> table_data=oc.Connect();
						HashMap<String,String> encode_table=oc.ReadEncodeTable();
						HashMap<String,String> event_type_code_table=oc.ReadEventtypeEncodeTable();
						
						int event_type=Integer.valueOf(event_type_code_table.get(tb_name));
						
						Iterator<Entry<Integer,HashMap<String,String>>> it1=table_data.entrySet().iterator();
						int rows=0;
						while(it1.hasNext())
						{
							//data of a row
							rows++;
							System.out.println(rows);
							Entry<Integer,HashMap<String,String>> entry=it1.next();
							HashMap<String,String> kv=entry.getValue();
							Iterator<Entry<String,String>> it2=kv.entrySet().iterator();
							
							long event_time=System.currentTimeMillis()/1000;
							int[] a=new int[4];
							for(int i=0;i<4;i++)
							{
								Random random=new Random();
								a[i]=random.nextInt(65536);
							}
							rowkey_generator rkg=new rowkey_generator(a[0],a[1],event_type,event_time,a[2],a[3]);
							
							while(it2.hasNext())
							{
								Entry<String,String> ent=it2.next();
								
								String key=ent.getKey();
								String value=ent.getValue();
								byte[] data=Bytes.toBytes(value);
								//System.out.println(key+"\t"+encode_table.get(key));
								
								int code=Integer.parseInt(encode_table.get(key));
								qualifier_generator qg=new qualifier_generator(event_type,code);
								di.put(tb_name, rkg.row_key, columnFamily, qg.qualifier,data);
								text_oracle_data.append(rows+"\t"+key+"\t"+value+"\n");
							}
						}
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
				else
				{
					text_oracle_data.setText(result);
				}
			}
		});	
		
		choose_file_btn.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				FileDialog fd = new FileDialog(shell);
				fd.open();
				String os=System.getProperty("os.name");
				String filename=fd.getFileName();
				String filepath=fd.getFilterPath();
				if(os.toLowerCase().startsWith("win"))
				{
					filepath=filepath.replaceAll("\\\\", "/");
				}
				System.out.println(filepath);
				text_file_names.setText(filepath+"/"+filename);
			}
		});
		
		choose_path_btn.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				FileDialog fd = new FileDialog(shell);
				fd.open();
				String os=System.getProperty("os.name");
				String filename=fd.getFileName();
				String filepath=fd.getFilterPath();
				if(os.toLowerCase().startsWith("win"))
				{
					filepath=filepath.replaceAll("\\\\", "/");
				}
				System.out.println(filepath);
				text_path_name.setText(filepath+"/"+filename);
			}
		});
		
		file_import_confirm_btn.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				String file_name=text_file_names.getText();
				String path_name=text_path_name.getText();
				
				CsvDataImporter cdi=new CsvDataImporter();
				
				//get event type
				int event_type;
				String[] tmp=file_name.split("/");
				String[] temp=tmp[tmp.length-1].split("_"); //get name of data table
				event_type=cdi.GetEventType(temp[0],text_encode_data);
				
				try {
					HashMap<String,Integer> codetable=cdi.ReadEncodeTable(path_name, text_encode_data);
					String table_name=text_file_tb_name.getText();
					String columnFamily=text_file_columnFamily.getText();
					if(!table_name.isEmpty()&&!columnFamily.isEmpty())
					{
						cdi.csv_importer(file_name, path_name, event_type,codetable,
								table_name,columnFamily,text_file_data, text_encode_data);
					}
					else
					{
						text_file_data.append("±íÃû»òÁÐ´ØÃûÎª¿Õ£¡\n");
					}
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		});
	}
}
