package ptt_rank;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MysqlDB
{
	Connection con;
	PreparedStatement ps;
	public MysqlDB()
	{
		try
		{
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection("jdbc:mysql://localhost:3306/DATA","root","km0049!!!");
		}
		catch(Exception ee)
		{
			System.out.print(ee.getMessage());
		}
	}
	public ResultSet query(String query)
	{
		System.out.print(query+"\n");
		try
		{
			ps = con.prepareStatement(query);
			return ps.executeQuery();
		} 
		catch (Exception ee)
		{
			System.out.println(ee.getMessage());
		}
		return null;
	}
}