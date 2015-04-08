package storm.helloworld;

import java.util.HashMap;
import java.util.Map;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/** 
 * @author blogchong
 * @Blog   www.blogchong.com
 * @email  blogchong@gmail.com
 * @QQ_G   191321336
 * @version 2014��11��9�� ����10:09:43
 */

@SuppressWarnings("serial")
public class WordCountBolt implements IRichBolt  {
	
	/**
	 * ���bolt���մ�normalizer����ĵ�������
	 * Ȼ����ݱ����ص�һ��map�У�ʵʱ�Ľ�ͳ�ƽ����ȥ
	 */
		
	Integer id;
	String name;
	Map<String, Integer> counters;

	private OutputCollector collector;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.collector = collector;
		
		//newһ��hashmapʵ��
		this.counters = new HashMap<String, Integer>();
		
		this.name = context.getThisComponentId();
		
		this.id = context.getThisTaskId();
		
	}

	@Override
	public void execute(Tuple input) {
		String str = input.getString(0);
		
		if (!counters.containsKey(str)) {
			counters.put(str, 1);
		} else {
			//����map���м�¼�������+1
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}
		
		String send_str = null;
		
		int count = 0;
		
		for (String key : counters.keySet()) {
			if (count == 0) {
				send_str = "[" + key + " : " + counters.get(key) + "]";
			} else {
				send_str = send_str + ", [" + key + " : " + counters.get(key) + "]";
			}
			
			count++;
			
		}
		
		send_str = "The count:" + count + " #### " + send_str; 
		
		this.collector.emit(new Values(send_str));
		
		
	}

	@Override
	public void cleanup() {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("send_str"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
