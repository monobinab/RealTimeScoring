import storm
import logging

class SignalSpoutBase(storm.Spout):
	data = []

	def initialize(self, conf, context):
		y = 'http://semantictec.com/message/consume?topic=user.activities.signal&size=100&consumerGroup=analytics&timeout=10'
		for element in range(100):
			self.data.append(y)
        	
	def next_tuple(self):
		for url in self.data:
			storm.emit([url])

	def ack(self, tup_id):
		pass  # if a tuple is processed properly, do nothing

	def fail(self, tup_id):
		pass  # if a tuple fails to process, do nothing
