package storm.helloworld;

import java.util.Map;
import storm.base.MacroDef;
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
public class WordNormalizerBolt implements IRichBolt  {
	
	/**
	 * �ò��ֽ�spout���͹�����һ�м�¼���б�׼������
	 * ������¼��ֳɵ��ʣ�����ֻȡdomain�ļ��еĵ�һ�к����һ��(��ʽ�Ǳ�׼��)
	 */

	private OutputCollector collector;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		
	}

	@Override
	public void execute(Tuple input) {
		String sentence = input.getString(0);
		//��'/t'���
		String[] words = sentence.split(MacroDef.FLAG_TABS);
		//ֻ�ѵ�һ�к����һ�з��ͳ�ȥ

		if (words.length != 0) {
			
			String domain = words[0];
			String[] do_word = domain.split("\\.");
			
			for (int i = 0; i < do_word.length; i++) {
				String word = do_word[i].trim();
				//������Ҳ��ֳɵ���
				this.collector.emit(new Values(word));
			}
			
			//�����ֱ�ӷ��ͳ�ȥ
			String word = words[4].trim();
			this.collector.emit(new Values(word));
		}
		
	}

	@Override
	public void cleanup() {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
