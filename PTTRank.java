package ptt_rank;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import main.KafkaTopicProducer;

public class PTTRank
{
	MysqlDB mysql;
	int interval, z;
	int[][] record;
	Date start, end;
	SimpleDateFormat pdf, odf;
	ArrayList<String> keywords, name;
	String timeQuery, orig_timeQuery;
	public PTTRank()
	{
		mysql = new MysqlDB();
		pdf = new SimpleDateFormat("yyyyMMddHHmmss");
		odf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		keywords = new ArrayList<String>();
		name = new ArrayList<String>();
		record = new int[101][101];
		z=0;
		
	}
	public void ranking(String _query) throws ParseException, SQLException
	{
		int i;
		String q;
		ResultSet tmp;
		Calendar cc;
		String[] str = _query.split(",");
		start = pdf.parse(str[0]);
		end = pdf.parse(str[1]);
		interval = Integer.parseInt(str[2]);// in minutes
		orig_timeQuery = timeQuery = "(TIME>='"+odf.format(start)+"' and TIME<='"+odf.format(end)+"')";
		//get all keywords be ranked
		q = "select WORD_ID,sum(COUNT) as cnt from RELATION where "+timeQuery+" group by WORD_ID order by cnt desc limit 100";
		tmp = mysql.query(q);
		while(tmp.next())
		{
			keywords.add(tmp.getString("WORD_ID"));
		}
		//query each interval
		while(start.before(end))
		{
			timeQuery = "(TIME>='"+odf.format(start)+"' and TIME<='";
			cc = Calendar.getInstance();
			cc.setTime(start);
			cc.add(Calendar.MINUTE, interval);
			start = cc.getTime();// add interval
			timeQuery += odf.format(start)+"')";
			for(i=0;i<keywords.size();i++)
			{
				q = "select sum(COUNT) as cnt from RELATION where "+timeQuery+"and WORD_ID='"+keywords.get(i)+"' ";
				tmp = mysql.query(q);
				if(tmp.next())
					record[z][i]=Integer.parseInt(tmp.getString("cnt")==null?"0":tmp.getString("cnt"));
			}
			z++;
		}
		//get keyword's name
		q = "select sum(COUNT) as cnt,WORD from RELATION,GLOSSARY where "+orig_timeQuery+" and WORD_ID=GLOSSARY.NO  group by WORD_ID order by cnt desc limit 100";
		tmp = mysql.query(q);
		while(tmp.next())
		{
			name.add(tmp.getString("WORD"));
		}
		KafkaTopicProducer.getInstance().send("ptt_rank_request",  getJSON());
	}
	public String getJSON()
	{
		StringBuilder result_str = new StringBuilder("");
		int i,j;
		result_str.append("{\"Rank\":");
		result_str.append("[");
		for(i=0;i<z;i++)
		{
			if(i>0)
				result_str.append(",");
			result_str.append("{\"Keywords\":[");
			for(j=0;j<keywords.size();j++)
			{
				if(j>0)
					result_str.append(",");
				result_str.append("{\"Word\":\""+name.get(j)+"\",\"Count\":"+record[i][j]+"}\n");
			}
			result_str.append("]}\n\n");
		}
		result_str.append("]");
		result_str.append("}");
		System.out.print(result_str);
		return result_str.toString();
	}
}
