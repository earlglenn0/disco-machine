const app = require('express')()
const http = require('http').createServer(app)
const { formatState } = require('./utils')
const config = require('./config')
const kafka = require('kafka-node')
const Producer = kafka.Producer
const Consumer = kafka.Consumer
const client = new kafka.KafkaClient(config.kafka_server)
const consumer = new Consumer(client, [{ topic: config.kafka_topic_consume, partition: 0 }], { autoCommit: false })
const producer = new Producer(client)
const { Machine, interpret } = require('xstate')
const discoConfig = require('./discoConfig')
const discoImplementation = require('./discoImplementation')
const service  = interpret(Machine(discoConfig, discoImplementation))
service.start()

producer.on('ready', () => {
  console.log('producer ready')
  service.onTransition(state => {
    const payload = {
      ...formatState(state.value),
      ...state.context,
      isBroken: state.value === 'broken'
    }
    const payloads = [
      {
        topic: config.kafka_topic_produce,
        messages: [ JSON.stringify(payload) ]
      }
    ]
    producer.send(payloads, (err, data) => {
      if (err) {
        console.log('producer failed')
      } else {
        console.log('producer succeeded')
      }
    })
  })
  consumer.on('message', (message) => {
    console.log({ message })
    service.send(message.value)
  })
})

http.listen(3002, () => {
  console.log('listening on *:3002');
});