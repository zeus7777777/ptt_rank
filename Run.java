package ptt_rank;

import java.util.Map;

import main.GlobalConfigMap;
import main.KafkaTopicConsumer;
import ptt_rank.KafkaListener;

public class Run
{
	public static void main(String[] args)
	{
		GlobalConfigMap config = new GlobalConfigMap();
		Map<String, String> zkMap = config.getGlobalConfigMap();
		KafkaTopicConsumer ktc = new KafkaTopicConsumer(
				"192.168.1.104:2181", 
				"PTT", 
				"ptt_rank_request", 
				new KafkaListener()
		);
	}

}
