

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBTable implements Serializable {
	//ArrayList<DBPage> arrPages;
	String Name ;
String path;
//int count;
	public DBTable(String Name) {
		Vector<DBPage> arrPages=new Vector<DBPage>();
		this.path="src/main/resources/data/Table_"+Name+".class";
		seralize(arrPages);
		this.Name=Name;
//count=0;
		//System.out.println(Name);


	}

	public  void seralize(Vector<DBPage> v){


		try {
			File myObj = new File(path);
			if(myObj.createNewFile()){
				Vector<DBPage>tembawy=new Vector<DBPage>();
				seralize(tembawy);
			}

		} catch (IOException e) {
			System.out.println("An error occurred.");
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
	public  Vector<DBPage> deseralize(){
		Vector<DBPage> v=new Vector<DBPage>();
		try {
			FileInputStream fileIn = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			v = (Vector<DBPage>) in.readObject();
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


	public static int search(Vector<DBPage> arr, Object c) {

	//	Collections.binarySearch(arrPages,c);
		int first=0;
		int last= arr.size()-1;
		
		 int mid = (first + last)/2;  
		   while( first <= last ){  
		      if ( compareClust(arr.get(mid).min,c)==-1){  
		    	  if(compareClust(arr.get(mid).max,c)==1||compareClust(arr.get(mid).min,c)==0)
		    		  return mid;
		    	  else
		        first = mid + 1;     
		      }else if ( compareClust(arr.get(mid).min,c)==0 ){  
		    	  //if there are O.Fs
		        return mid; 
		        
		      }else{  
		         last = mid - 1;  
		      }  
		      mid = (first + last)/2;  
		   }  
		   if ( first > last ){  
		      //System.out.println("Element is not found!");  
		   }  
		return -1;
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

//unique key
	public void DBTInsert(DBTuple t) throws DBAppException{
		Object c=t.clust;
		//System.out.println(Name+"/"+count);
		//count=count+1;

		Vector<DBPage>arrPages=deseralize();
	//	System.out.println(c);
		if (arrPages.size()==0) {
			DBPage p=new DBPage (Name,c,c,0,0);
			arrPages.add(p);
			p.pInsert(t);
			seralize(arrPages);
			return;

		}
		else //there are pages
		{
			int pinsertnum = 0;
			pinsertnum=search(arrPages,c);//i must check enaha between el minimum wel maximum, wala la2 msh shart
			if(pinsertnum<0) {
				if(compareClust(t.clust,arrPages.get(0).min)==-1)
					pinsertnum=0;
				else
					pinsertnum=arrPages.size()-1;
			}
			if(arrPages.get(pinsertnum).number==arrPages.get(pinsertnum).maxC) {
				if(arrPages.size()>pinsertnum+1) { //if not the last one
					if(arrPages.get(pinsertnum+1).number==arrPages.get(pinsertnum).maxC) {//if next is full
						if(arrPages.get(pinsertnum).pGet(c)!=null)
							throw new DBAppException("There already exists a tuple with the same clustering key");
						arrPages.get(pinsertnum).pInsert(t);
						seralize(arrPages);
							return;
					}
					else {//next is not full , i will shift one record
						DBTuple temp=arrPages.get(pinsertnum).pGet(arrPages.get(pinsertnum).max);
						arrPages.get(pinsertnum).pDelete(temp);
						arrPages.get(pinsertnum).pInsert(t);
						arrPages.get(pinsertnum+1).pInsert(temp);
						seralize(arrPages);
						return;

					}

				}
				else {//this page is the last page
					if(compareClust(arrPages.get(pinsertnum).max,c)!=-1) {//the inserted tuple is not within the range of the last page
						if(arrPages.get(pinsertnum).pGet(c)!=null) {
							//System.out.println(arrPages.get(pinsertnum).pGet(c).data+"//"+c);
							throw new DBAppException("There already exists a tuple with the same clustering key");
						}
					DBPage newlast =new DBPage(Name,arrPages.get(pinsertnum).max,arrPages.get(pinsertnum).max,arrPages.size(),0);
					DBTuple temp=arrPages.get(pinsertnum).pGet(arrPages.get(pinsertnum).max);
					arrPages.get(pinsertnum).pDelete(temp);
					arrPages.get(pinsertnum).pInsert(t);
					//DBTInsert(newlast);
					arrPages.add(newlast);
					newlast.pInsert(temp);
						seralize(arrPages);
					return;
					}
					else {
						if(arrPages.get(pinsertnum).pGet(c)!=null)
							throw new DBAppException("There already exists a tuple with the same clustering key");

					DBPage newlast =new DBPage(Name,c,c,arrPages.size(),0);
					DBTuple temp=arrPages.get(pinsertnum).pGet(arrPages.get(pinsertnum).max);
					arrPages.get(pinsertnum).pDelete(temp);
					arrPages.get(pinsertnum).pInsert(t);
					//DBTInsert(newlast);
					arrPages.add(newlast);
					newlast.pInsert(temp);
						seralize(arrPages);
					return;
					}

				}
		}
			else //page to be inserted is not full
			{
				//if(arrPages.get(pinsertnum).pGet(c)!=null)
					//throw new DBAppException("There already exists a tuple with the same clustering key");

				arrPages.get(pinsertnum).pInsert(t);
				seralize(arrPages);
				return;
			}
		/**else
		{

		for(int i = 0;i<arrPages.size();i++) {

			if(arrPages.get(i).number!=200) {
				if(arrPages.get(i).pGet(c)==null) {
				arrPages.get(i).pInsert(t);

				return;}
				else
					throw new DBAppException("There already exists a tuple with the same clustering key");}

			else
			{
		Object min =arrPages.get(i).min;
		Object max =arrPages.get(i).max;

			if((c instanceof Integer )&&(Integer)c<((Integer) max))
			{
				if(arrPages.get(i).pGet(c)!=null)
					throw new DBAppException("There already exists a tuple with the same clustering key");

				DBTuple temp=arrPages.get(i).pGet(max);
				arrPages.get(i).pDelete(max);
				arrPages.get(i).pInsert(t);
				DBTInsert(temp);
				return;

			}




			else if(c instanceof String&& (((String)c).compareTo((String) max)>0))

			{
				if(arrPages.get(i).pGet(c)!=null)
					throw new DBAppException("There already exists a tuple with the same clustering key");

				DBTuple temp=arrPages.get(i).pGet(max);
				arrPages.get(i).pDelete(max);
				arrPages.get(i).pInsert(t);
				DBTInsert(temp);	return;

			}







			else if(c instanceof Double &&((Double)c)<(Double)max)

			{
				if(arrPages.get(i).pGet(c)!=null)
					throw new DBAppException("There already exists a tuple with the same clustering key");

				DBTuple temp=arrPages.get(i).pGet(max);
				arrPages.get(i).pDelete(max);
				arrPages.get(i).pInsert(t);
				DBTInsert(temp);	return;

			}


			else if((c instanceof Date)&&((Date)c).compareTo((Date)min)<0)
			{
				if(arrPages.get(i).pGet(c)!=null)
					throw new DBAppException("There already exists a tuple with the same clustering key");

				DBTuple temp=arrPages.get(i).pGet(max);
				arrPages.get(i).pDelete(max);
				arrPages.get(i).pInsert(t);
				DBTInsert(temp);	return;

			}
		}}
		DBPage p=new DBPage (Name,c,c,arrPages.size());
		arrPages.add(p);
		p.pInsert(t);
		return;
	}
**/
	}}

	public void DBTDelete(DBTuple t) throws DBAppException{
		Vector<DBPage>arrPages=deseralize();
		for(int i=0;i<arrPages.size();i++) {
			arrPages.get(i).pDelete(t);
			if(arrPages.get(i).number==0) {

				if(arrPages.get(i).nextof==null) {
					arrPages.remove(i);
					i--;
				}
				else {
					DBPage temp=arrPages.get(i).nextof;
					arrPages.remove(i);
					arrPages.add(i, temp);
				}
			}

		}seralize(arrPages);
	}
	public void DBTUpdate(DBTuple t) {

		Vector<DBPage>arrPages=deseralize();
		for (int i=0;i<arrPages.size();i++) {
			arrPages.get(i).min = arrPages.get(i).getMin();
			arrPages.get(i).max = arrPages.get(i).getMax();
		}
		int pupdatenum=search(arrPages,t.clust);
		if(pupdatenum<0) {
			if(compareClust(t.clust,arrPages.get(0).min)==-1)
				pupdatenum=0;
			else
				pupdatenum=arrPages.size()-1;
		}

	    arrPages.get(pupdatenum).pUpdate(t);
seralize(arrPages);
	}
}
