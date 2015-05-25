package ptt_rank;

import main.MsgReceiver;

public class KafkaListener extends MsgReceiver
{
	PTTRank pttr;
	public void execute(String message)
	{
		try
		{
			pttr = new PTTRank();
			pttr.ranking(message);
		}
		catch(Exception ee)
		{
			System.out.print(ee.getMessage());
		}
	}
}
