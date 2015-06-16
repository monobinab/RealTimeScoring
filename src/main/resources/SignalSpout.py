import storm
import requests
import json
import ast


class SignalSpoutBase(storm.Spout):
	y = requests.get('http://semantictec.com/message/consume?topic=user.activities.signal&size=100&consumerGroup=analytics&timeout=10')
	def process(self):
		for element in json.loads(self.y.content):
			try:
				values = ast.literal_eval(element['value'])
				storm.emit([values['channel'],values['products'],values['signalTime'],values['user']['sywrId'],values['user']['uuid']])
			except:
				pass
		
