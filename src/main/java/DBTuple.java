
import java.io.Serializable;
import java.util.*;
public class DBTuple implements Comparable<DBTuple> , Serializable {
	Object clust;
	String data;

	public DBTuple(String data,Object clust) {
		this.clust=clust;
		this.data=data;

	}
	  public int compareTo(DBTuple two ) {

		  if(this.clust instanceof Integer) {
			  int diff = ((Integer)this.clust)- ((Integer)two.clust);//<-- compare ints
			  if (diff<0) {
		            return -1;
		        }
		        if (diff==0) {
		            return 0;
		        }
		        return 1;
		  }
		  if(this.clust instanceof Double) {
			  double diff = ((Double)this.clust)- ((Double)two.clust);//<-- compare ints
			  if (diff<0) {
		            return -1;
		        }
		        if (diff==0) {
		            return 0;
		        }
		        return 1;
		  }
		  if(this.clust instanceof String) {
			  int diff = ((String)this.clust).compareTo((String)two.clust);//<-- compare ints
			  if (diff<0) {
		            return 1;
		        }
		        if (diff==0) {
		            return 0;
		        }
		        return -1;
		  }

		  if(this.clust instanceof Date) {
			  int diff = ((Date)this.clust).compareTo((Date)two.clust);//<-- compare ints
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

}
