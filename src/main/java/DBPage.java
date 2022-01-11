

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.*;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Date;
import java.util.*;
//import main.java;

public class DBPage implements Serializable {
	Object min;
	Object max;
	String Table;
	//Vector<DBTuple> records;
	String path;
	int number;
	int pnum;
	int of;
	DBPage nextof;
	int maxC;

	public DBPage(String T,Object min,Object max,int pnum,int of) {

		//"src/main/resources/metadata.csv"
		this.max=max;
		this.min=min;
		Table=T;
		this.pnum=pnum;
		this.of=of;
		//check .class
		path="src/main/resources/data/"+T+"_"+this.pnum+"_"+of+".class";
//System.out.println(path);
		//BOS HENA KEDA
		Vector<DBTuple> v=new Vector<DBTuple>();




		//serializing page
		this.seralize(v);

		//records=new Vector<DBTuple>();
		Properties grades = new Properties();
		FileInputStream in = null;
		try {
			in = new FileInputStream("src/main/resources/DBApp.config");
//System.out.println("keh");
			grades.load(in);

			in.close();
			for (String key : grades.stringPropertyNames()) {
				if(key.equals("MaximumRowsCountinPage")){
				maxC = (int)Integer.parseInt(grades.getProperty(key));
				//System.out.println(maxC);
					}
			}

		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	public static int compareClust(Object c1,Object c2) {
		 if(c1 instanceof Integer ) {
			  int diff = ((Integer)c1)- ((Integer)c2);//<-- compare ints
			  if (diff<0) {
		            return -1;
		        }
		        if (diff==0) {
		            return 0;
		        }
		        return 1;
		  }
		  if(c1 instanceof Double) {
			  double diff = ((Double)c1)- ((Double)c2);//<-- compare ints
			  if (diff<0) {
		            return -1;
		        }
		        if (diff==0) {
		            return 0;
		        }
		        return 1;
		  }
		  if(c1 instanceof String) {
			  int diff = ((String)c1).compareTo((String)c2);//<-- compare ints
			  if (diff<0) {
		            return 1;
		        }
		        if (diff==0) {
		            return 0;
		        }
		        return -1;
		  }

		  if(c1 instanceof Date) {
			  int diff = ((Date)c1).compareTo((Date)c2);//<-- compare ints
			  if (diff<0) {
		            return 1;
		        }
		        if (diff==0) {
		            return 0;
		        }
		        return -1;
		  }
		return 0;

	}

	public void pInsert(DBTuple T) {
		Vector<DBTuple> v= new Vector<DBTuple>();
		//deserializing page


		if(this.number<maxC) {
			v=this.deseralize();
		number++;
		// instead of this: records.add(T);
		v.add(T);
			this.seralize(v);
			max=getMax();
			min=getMin();

	 Collections.sort(v);
	// max=v.get(v.size()-1).clust;
	 //min=v.get(0).clust;

	 
	 //this.seralize(v);
		}
		if(this.number>=maxC) { //overflow
			if(this.nextof==null) { //there are no O.F. pages
				DBPage newof= new DBPage(Table,T.clust,T.clust,this.pnum,of+1);
				newof.pInsert(T);
				nextof=newof;
				this.seralize(v);


			}
			else {//there is O.F. pages
				nextof.pInsert(T);
				this.seralize(v);

			}
			max=getMax();
			min=getMin();
		}

	 //

		    // Using Collection.sort(List list, Comparator c) static operation we can sort Vector elements using a Comparator

	 //serializing page


			}
	public void pDelete (DBTuple t) {

		Vector<DBTuple> records= new Vector<DBTuple>();
		//deserializing page
		records=this.deseralize();

		for(int i=0;i<records.size();i++) {
			String d=records.get(i).data;
			 String[] tdata = d.split(",");
			 String[] del = (t.data).split(",");
			 boolean flag=true;

			 for(int j=0;j<del.length;j++) {

				 if((!del[j].equals("null"))&&(!del[j].equals(tdata[j]))) {
					 flag=false;
					 break;

				 }
			 }
			 if(flag) {
			 	//System.out.println("deleted");
				 records.remove(i);
				 i--;
				 this.number--;
			 }


		}



		if(nextof!=null) {

			nextof.pDelete(t);
			if(nextof.number==0)
				this.nextof=this.nextof.nextof;

		}
		if(records.size()>0){
max=getMax();
		min=getMin();}
		this.seralize(records);
	}


	public DBTuple pGet (Object c) {
		Vector<DBTuple> records= new Vector<DBTuple>();
		//deserializing page
		records=this.deseralize();
		if (c==null)
			return null;
		/*if(c instanceof Integer) {
			for(int i = 0 ; i < records.size() ; i++){
				if(((Integer) c)-(Integer)records.get(i).clust>-0.0001&&((Integer) c)-(Integer)records.get(i).clust<0.0001)
			    {
			    	this.seralize(records);
			    	return records.get(i);
			    }
			}
		}
		if(c instanceof Double) {
			for(int i = 0 ; i < records.size() ; i++){
				//System.out.println(c+"///"+(Double)records.get(i).clust);
			    if(((Double) c)-(Double)records.get(i).clust>-0.0001&&((Double) c)-(Double)records.get(i).clust<0.0001){
				//	System.out.println(c+"/jajaja/"+(Double)records.get(i).clust);
			    	this.seralize(records);
			    	return records.get(i);
			    }
			}
		}
		if(c instanceof String) {
			for(int i = 0 ; i < records.size() ; i++){
			    if(((String)c).equals((String)records.get(i).clust))
			    {
			    	//System.out.println(c+"///"+(String)records.get(i).clust);
			    	this.seralize(records);
			    	return records.get(i);
			    }
			}
		}
		if(c instanceof Date) {
			for(int i = 0 ; i < records.size() ; i++){
			    if(((Date)c).equals((Date)records.get(i).clust))
			    {
			    	this.seralize(records);
			    	return records.get(i);
			    }
			}
		}*/
		int l = 0, r = records.size() - 1;
		while (l <= r) {
			int m = l + (r - l) / 2;

			// Check if x is present at mid
			if (compareClust(records.get(m).clust,c)==0)
			{
				//System.out.println(records.get(m)+"/////"+c);
				this.seralize(records);
				return records.get(m);
			}

			// If x greater, ignore left half
			if (compareClust(records.get(m).clust,c)==-1)
				l = m + 1;

				// If x is smaller, ignore right half
			else
				r = m - 1;
		}

		// if we reach here, then element was
		// not present




		if(this.nextof!=null) {
			return nextof.pGet(c);
		}
		else {
			return null;
		}


	}

public  void seralize(Vector<DBTuple> v){



	try {
		File myObj = new File(path);
		if(myObj.createNewFile()){
			Vector<DBTuple>tembawy=new Vector<DBTuple>();
			seralize(tembawy);
		}

	} catch (IOException e) {
		//System.out.println("An error occurred.");
		e.printStackTrace();
	}

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
public  Vector<DBTuple> deseralize(){
	Vector<DBTuple> v=new Vector<DBTuple>();
	try {
        FileInputStream fileIn = new FileInputStream(path);
        ObjectInputStream in = new ObjectInputStream(fileIn);
         v = (Vector<DBTuple>) in.readObject();
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
public void pUpdate(DBTuple t) {
	//DBTuple update=pGet(t.clust);
	Object c=t.clust;
//	System.out.println(c);
	Vector<DBTuple> records= new Vector<DBTuple>();
	//deserializing page
	records=this.deseralize();
	if(c instanceof Integer) {
		for(int i = 0 ; i < records.size() ; i++){

			if(((Integer) c)-(Integer)records.get(i).clust>-0.1&&((Integer) c)-(Integer)records.get(i).clust<0.1)
		    {
		    	String[] old=records.get(i).data.split(",");
		    	String[] gedid=t.data.split(",");
		    	for(int j=0;j<gedid.length;j++) {
		    		if(!gedid[j].equals("null")) {
		    			old[j]=gedid[j];
		    		}
		    		
		    	}

		    	String gToBe="";
		    	for(int k=0;k<old.length;k++) {
		    		if(k==0) {
		    			gToBe=""+old[k];
		    		}
		    		else
		    		{
		    			gToBe=gToBe+","+old[k];
		    		}
		    	}
		    	records.get(i).data=gToBe;
		    	this.seralize(records);
		    	return;
		    }
		}
	}
	if(c instanceof Double) {
		for(int i = 0 ; i < records.size() ; i++){
		//	System.out.println(c+"///"+records.get(i).clust);
			if(((Double) c)-(Double)records.get(i).clust>-0.0001&&((Double) c)-(Double)records.get(i).clust<0.0001)
		    {

		    	String[] old=records.get(i).data.split(",");
		    	String[] gedid=t.data.split(",");
		    	for(int j=0;j<gedid.length;j++) {
		    		if(!gedid[j].equals("null")) {
		    			old[j]=gedid[j];
		    		}
					//System.out.println("zizid");
		    	}
		    	String gToBe="";
		    	for(int k=0;k<old.length;k++) {
		    		if(k==0) {
		    			gToBe=""+old[k];
		    		}
		    		else
		    		{
		    			gToBe=gToBe+","+old[k];
		    		}
		    	}
		    	records.get(i).data=gToBe;
		    	this.seralize(records);
		    	return;
		    
		    }
		}
	}
	if(c instanceof String) {
		for(int i = 0 ; i < records.size() ; i++){
		    if(((String)c).equals((String)records.get(i).clust))
		    {
		    	String[] old=records.get(i).data.split(",");
		    	String[] gedid=t.data.split(",");
		    	for(int j=0;j<gedid.length;j++) {
		    		if(!gedid[j].equals("null")) {
		    			old[j]=gedid[j];
		    		}
		    		
		    	}
				//System.out.println("zizis");
		    	String gToBe="";
		    	for(int k=0;k<old.length;k++) {
		    		if(k==0) {
		    			gToBe=""+old[k];
		    		}
		    		else
		    		{
		    			gToBe=gToBe+","+old[k];
		    		}
		    	}
		    	records.get(i).data=gToBe;
		    	this.seralize(records);
		    	return;
		    
		    }
		}
	}
	if(c instanceof Date) {
		for(int i = 0 ; i < records.size() ; i++){
		    if(((Date)c).equals((Date)records.get(i).clust));
		    {

		    	String[] old=records.get(i).data.split(",");
		    	String[] gedid=t.data.split(",");
		    	for(int j=0;j<gedid.length;j++) {
		    		if(!gedid[j].equals("null")) {
		    			old[j]=gedid[j];
		    		}
		    		
		    	}

		    	String gToBe="";
		    	for(int k=0;k<old.length;k++) {
		    		if(k==0) {
		    			gToBe=""+old[k];
		    		}
		    		else
		    		{
		    			gToBe=gToBe+","+old[k];
		    		}
		    	}
		    	records.get(i).data=gToBe;
		    	this.seralize(records);
		    	return;
		    
		    }
		}
	}
	if(this.nextof!=null) {
		nextof.pUpdate(t);
	}
	else {

	}
	return ;

	
}

	public Object getMax() {
		Vector<DBTuple> v=deseralize();

		Object tmax=v.get(0).clust;
		for (int i=0;i<v.size();i++){
			if(compareClust(v.get(i).clust,max)==1){
				tmax=v.get(i).clust;
			}
		}
		if(nextof!=null){
		Object nmax=this.nextof.getMax();
		if(compareClust(nmax,tmax)==1)
		tmax=nmax;}
		return tmax;
	}
	public Object getMin() {
		Vector<DBTuple> v=deseralize();
		Object tmax=v.get(0).clust;
		for (int i=0;i<v.size();i++){
			if(compareClust(v.get(i).clust,max)==-1){
				tmax=v.get(i).clust;
			}
		}
		if(nextof!=null){
			Object nmax=this.nextof.getMin();
			if(compareClust(nmax,tmax)==-1)
				tmax=nmax;}
		return tmax;
	}
}

