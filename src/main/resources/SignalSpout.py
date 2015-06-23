import storm
import logging
import requests
import json
import ast 

class SignalSpoutBase(storm.Spout):
	data = []

	def initialize(self, conf, context):
		y = 'http://semantictec.com/message/consume?topic=user.activities.signal&size=100&consumerGroup=analytics&timeout=10'
		z = requests.get(y)
		x = json.loads(z.content)
		for element in x:
			values = ast.literal_eval(element['value'])
			self.data.append([values['channel'],values['products'],values['searchTerm'],values['signalTime'],values['source'],values['taxonomy'],values['type'],values['user']['uuid']])
        	
	def nextTuple(self):
		for element in self.data:
			storm.emit(element)

	def ack(self, tup_id):
		pass  # if a tuple is processed properly, do nothing

	def fail(self, tup_id):
		pass  # if a tuple fails to process, do nothing
