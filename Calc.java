package pigudf;

import java.io.IOException;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class Calc extends FilterFunc {
	public Boolean exec (Tuple input) throws IOException
	{
		try
		{
			int value1 = Integer.parseInt((String) input.get(0));
			int value2 = Integer.parseInt((String) input.get(1));
			
			if (value1 == 0 || value2 == 0 )
			{	
				System.out.println("zero values");
				System.exit(1);
			}
			return ((value1/value2)>=0.8);
		}
		catch (Exception e)
		{
			System.out.println("something wrong"+e.getMessage());
		}
		return null;
	}
}