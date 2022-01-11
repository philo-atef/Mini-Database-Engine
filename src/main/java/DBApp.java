

import java.io.*;
import java.util.*;
import java.text.*;
import com.opencsv.CSVReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DBApp implements DBAppInterface{
	//ArrayList<DBTable> arrTables;
String path="src/main/resources/data/DBApp.class";
	public  void seralize(Vector<DBTable> v){



		//serializing page
		try {
			FileOutputStream fileOut =
					new FileOutputStream(path);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(v);
			out.close();
			fileOut.close();
			// System.out.printf("Serialized data is saved in "+path);
		} catch (IOException i) {
			i.printStackTrace();
		}
		return ;
	}
	public  Vector<DBTable> deseralize(){
		Vector<DBTable> v=new Vector<DBTable>();
		try {
			FileInputStream fileIn = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			v = (Vector<DBTable>) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
			return null;
		} catch (ClassNotFoundException cne) {
			System.out.println(" class not found");
			cne.printStackTrace();
			return null;
		}
		return v;
	}

	public static DBTuple createTuple (String tableName,Hashtable<String, Object> x,Object c) {
		BufferedReader csvReader;
		try {
			csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

		String row;
		String temp = "";
	//	Set<String> keys = colNameValue.keySet();

		ArrayList<String> csvcol=new ArrayList<String> ();





		while ((row = csvReader.readLine()) != null) {
			String [] record = row.split(",");
			if(tableName.equals(record[0])){
				csvcol.add(record[1]);
			}
		}

	String str="";

		for(int i=0;i<csvcol.size();i++){
			if(x.get(csvcol.get(i))!= null) {

				if(str.equals(""))
					{str=str+x.get(csvcol.get(i));}
				else
					str=str+","+x.get(csvcol.get(i));
			}
			else

				if(str.equals(""))
					{str=str+"null";}
				else
					str=str+","+"null";
		}
		DBTuple x1=new DBTuple(str,c);
		return x1;
		}
		catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return null;


	}

	public  void init( ) {
		try {
			File myObj = new File(path);
			if(myObj.createNewFile()){
				Vector<DBTable> tembawy =new Vector<DBTable>();
				seralize(tembawy);
			}

		} catch (IOException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}

		//Vector<DBTable>arrTables=deseralize();
		 PrintWriter pw = null;
		//seralize(arrTables);
         try {

			 BufferedReader csvReader;

				 csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

				 String row;
				 String temp = "";

				 while ((row = csvReader.readLine()) != null) {
					 temp=temp+row+"\n";
					 // do something with the data
				 }
if(temp.equals("")){
	pw = new PrintWriter(new File("src/main/resources/metadata.csv"));
     StringBuilder builder = new StringBuilder();
     builder.append( "Table Name, Column Name, Column Type, ClusteringKey, Indexed, min, max");
     pw.write(builder.toString());
    // System.out.println("init");
     pw.close();}
         }
         catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			 e.printStackTrace();
		 }


	};

	;// this does whatever initialization you would like
	// or leave it empty if there is no code you want to
	// execute at application startup

	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value
	// htblColNameMin and htblColNameMax for passing minimum and maximum values
	// for data in the column. Key is the name of the column
	public void createTable(String strTableName,
	 String strClusteringKeyColumn,
	Hashtable<String,String> htblColNameType,
	Hashtable<String,String> htblColNameMin,
	Hashtable<String,String> htblColNameMax )
	 throws DBAppException {

		BufferedReader csvReader;
		try {
			csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

			String row;
			String temp = "";

			while ((row = csvReader.readLine()) != null) {
				temp=temp+row+"\n";
				// do something with the data
			}
			//System.out.println(temp);
			csvReader.close();
			PrintWriter pw = null;

			pw = new PrintWriter(new File("src/main/resources/metadata.csv"));

			StringBuilder builder = new StringBuilder();

			builder.append(temp);

			Set<String> keys = htblColNameType.keySet();
			for(String key: keys){
				String [] arr=new String [7];
				arr[0]=strTableName;
			//	System.out.println("Value of "+key+" is: "+htblColNameType.get(key));
				arr[1]=key;
				arr[2]=htblColNameType.get(key);
				if(key.equals(strClusteringKeyColumn)) {
					arr[3]="True";

				}
				else {
					arr[3]="False";
				}
				arr[4]="False";
				arr[5]=htblColNameMin.get(key);
				arr[6]=htblColNameMax.get(key);

				for(int i=0;i<arr.length;i++) {
					builder.append(arr[i]);
					if(i+1!=arr.length)
					builder.append(",");

				}
				builder.append('\n');
			}







		//System.out.println(builder.toString());

			pw.write(builder.toString());
		//	System.out.println(builder.toString());
			pw.flush();
			pw.close();
			DBTable n=new DBTable(strTableName);

Vector<DBTable> arrTables=deseralize();
arrTables.add(n);
seralize(arrTables);


		}catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}


	}


	@Override
	public void createIndex(String tableName, String[] columnNames) throws DBAppException {
		// TODO Auto-generated method stub

	}

		@Override

        public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
            BufferedReader csvReader;

			try {
				csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

				String row;
				String temp = "";
				Set<String> keys = colNameValue.keySet();

				ArrayList<String> csvcol=new ArrayList<String> ();

                //System.out.println(temp);


            while ((row = csvReader.readLine()) != null) {
                String [] record = row.split(",");
               // System.out.println(row);
                if(tableName.equals(record[0])){
                   // System.out.println(record[1]);
                    csvcol.add(record[1]);
                }
            }

            for(String key: keys) {
                if(!(csvcol.contains(key))) {
                    //System.out.println("yeezyzz");
                    throw new DBAppException();


                }
            }
            for(String key:csvcol) {
                if(!keys.contains(key)){
                    //System.out.println("yeezyzz000000000000000000");
                    throw new DBAppException("The entered column names do not match those of the inserted table name");
            }}

            csvReader.close();
    Object cluster=null;
            for(String key: keys) {
                Object cVal =colNameValue.get(key);
                csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
                while ((row = csvReader.readLine()) != null) {
                    String [] record = row.split(",");
                    if(tableName.equals(record[0])){

                        if(key.equals(record[1])) {

                            if(record[3].equals("True"))
                                cluster=cVal;

//System.out.println(cVal+"  "+record[2]);


							if((record[2].equals("java.lang.Integer"))) {

								if (! (cVal instanceof Integer))
									throw new DBAppException("Type of Key does not match that in the table");
								else if (!(((Integer) cVal) >= Integer.parseInt(record[5]) && ((Integer) cVal) <= Integer.parseInt(record[6])))
									throw new DBAppException("The Key value " + (String) cVal + " does not fall within the specified range");
							}

							else if((record[2].equals("java.lang.String"))) {

								if (!(cVal instanceof String))
									throw new DBAppException("Type of Key does not match that in the table");
								else if (!(((String) cVal).compareTo(record[5]) >= 0 && ((String) cVal).compareTo(record[6]) <= 0))
									throw new DBAppException("The Key value " + (String) cVal + " does not fall within the specified range");

							}



								if((record[2].equals("java.lang.Double"))) {

									if (!(cVal instanceof Double))
										throw new DBAppException("Type of Key does not match that in the table");
									else if (!(((Double) cVal) >= Double.parseDouble(record[5]) && ((Double) cVal) <= Double.parseDouble(record[6])))
										throw new DBAppException("The Key value " + (String) cVal + " does not fall within the specified range");


								}
									if((record[2].equals("java.lang.Date"))) {

										if (!(cVal instanceof Date))
											throw new DBAppException("Type of Key does not match that in the table");
										else
											try {
												if (!(((Date) cVal).compareTo(new SimpleDateFormat("yyyy/MM/dd").parse(record[5])) >= 0 && (((Date) cVal).compareTo(new SimpleDateFormat("yyyy/MM/dd").parse(record[6])) <= 0)))
													throw new DBAppException("The Key value " + (String) cVal + " does not fall within the specified range");
											} catch (ParseException e) {
												// TODO Auto-generated catch block
												e.printStackTrace();
											}
									}

                        }
                    }
                }
                csvReader.close();}
            Vector<DBTable> arrTables =deseralize();
           // System.out.println(arrTables.size());
            for(int i=0;i<arrTables.size();i++) {
                if(tableName.equals(arrTables.get(i).Name)) {

                    DBTuple t=createTuple(tableName,colNameValue,cluster);
                    arrTables.get(i).DBTInsert(t);
                    return;}
                }
                    // do something with the data
				seralize(arrTables);

            csvReader.close();


            }
            catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }


        }

	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException {
		BufferedReader csvReader;
		Object clust=null;

		try {
			csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

			String row;
			String temp = "";
			Set<String> keys = columnNameValue.keySet();

			ArrayList<String> csvcol=new ArrayList<String> ();

			//System.out.println(temp);


			while ((row = csvReader.readLine()) != null) {
				String [] record = row.split(",");
				//System.out.println(row);
				if(tableName.equals(record[0])){
				//	System.out.println(record[1]);
					csvcol.add(record[1]);
				}
			}

			for(String key: keys) {
				if(!(csvcol.contains(key))) {
					//System.out.println("yeezyzz");
					throw new DBAppException();


				}
			}
		Object cluster=null;
		for(String key: keys) {
			Object cVal =columnNameValue.get(key);
			csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((row = csvReader.readLine()) != null) {
				String [] record = row.split(",");
				if(tableName.equals(record[0])){

					if(key.equals(record[1])) {

						if(record[3].equals("True"))
							cluster=cVal;

						//System.out.println(cVal+"  "+record[2]);


						if((record[2].equals("java.lang.Integer"))) {

							if (! (cVal instanceof Integer))
								throw new DBAppException("Type of Key does not match that in the table");
							else if (!(((Integer) cVal) >= Integer.parseInt(record[5]) && ((Integer) cVal) <= Integer.parseInt(record[6])))
								throw new DBAppException("The Key value " + (String) cVal + " does not fall within the specified range");
						}

						else if((record[2].equals("java.lang.String"))) {

							if (!(cVal instanceof String))
								throw new DBAppException("Type of Key does not match that in the table");
							else if (!(((String) cVal).compareTo(record[5]) >= 0 && ((String) cVal).compareTo(record[6]) <= 0))
								throw new DBAppException("The Key value " + (String) cVal + " does not fall within the specified range");

						}



						if((record[2].equals("java.lang.Double"))) {

							if (!(cVal instanceof Double))
								throw new DBAppException("Type of Key does not match that in the table");
							else if (!(((Double) cVal) >= Double.parseDouble(record[5]) && ((Double) cVal) <= Double.parseDouble(record[6])))
								throw new DBAppException("The Key value " + (String) cVal + " does not fall within the specified range");


						}
						if((record[2].equals("java.lang.Date"))) {

							if (!(cVal instanceof Date))
								throw new DBAppException("Type of Key does not match that in the table");
							else
								try {
									if (!(((Date) cVal).compareTo(new SimpleDateFormat("yyyy/MM/dd").parse(record[5])) >= 0 && (((Date) cVal).compareTo(new SimpleDateFormat("yyyy/MM/dd").parse(record[6])) <= 0)))
										throw new DBAppException("The Key value " + (String) cVal + " does not fall within the specified range");
								} catch (ParseException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
						}

					}
				}
			}
			}
		
		
		
		
		
	
			csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

		//String row;
		
	//	Set<String> keys = colNameValue.keySet();

	//	ArrayList<String> csvcol=new ArrayList<String> ();





		while ((row = csvReader.readLine()) != null) {
			String [] record = row.split(",");
			if(tableName.equals(record[0])&&record[3].equals("True")){
				temp=record[2];
				break;
			}
		}
	//	System.out.println(temp+" //"+clusteringKeyValue);
		if(temp.equals("java.lang.Integer")) {
			 clust=Integer.parseInt(clusteringKeyValue);
		}
		else if(temp.equals("java.lang.Double")) {
			clust=Double.parseDouble(clusteringKeyValue);
		}
		else if(temp.equals("java.lang.String")) {
			clust=clusteringKeyValue;
		}
		else if(temp.equals("java.util.Date")) {
			try {

			/*	System.out.println(clusteringKeyValue);
				SimpleDateFormat format1 = new SimpleDateFormat("yyyy/MM/dd");

				String formatted = format1.format(clusteringKeyValue);
				clust = format1.parse(formatted);
		*/	clust=new SimpleDateFormat("yyyy-MM-dd").parse(clusteringKeyValue);
			}
			catch(ParseException e){
				e.printStackTrace();
			}
		}
		DBTuple t=createTuple(tableName,columnNameValue,clust);
		
		Vector<DBTable> arrTables=deseralize();
		for(int i=0;i<arrTables.size();i++) {
			if(tableName.equals(arrTables.get(i).Name)) {
				arrTables.get(i).DBTUpdate(t);
			}
		}
			seralize(arrTables);

	}
		catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
		// TODO Auto-generated method stub



Vector<DBTable> arrTables=deseralize();
		for(int i=0;i<arrTables.size();i++) {
			if(tableName.equals(arrTables.get(i).Name)) {

				DBTuple t=createTuple(tableName,columnNameValue,null);
				arrTables.get(i).DBTDelete(t);
				return;}
			}


seralize(arrTables);




	}
	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		// TODO Auto-generated method stub
		return null;
	}

}