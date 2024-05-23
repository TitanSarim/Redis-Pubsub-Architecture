import { Kafka,Producer } from "kafkajs";
import fs from 'fs'
import path from "path";
import prismaClient from "./prisma";

const kafka = new Kafka({
    brokers: ['kafka-a414280-sarimxahid123-7a0a.c.aivencloud.com:25656'],
    ssl: {
        ca: [fs.readFileSync(path.resolve('./src/services/ca.pem'), 'utf8')],
    },
    sasl: {
       username: 'avnadmin',
       password: 'AVNS_uvC0JX5T8m03idKYvu7' ,
       mechanism: "plain"
    }
})

let producer: null | Producer = null;

export async function createProducer() {
    if(producer) return producer;

    const _producer = kafka.producer()
    await _producer.connect()
    producer = _producer
    return producer
}

export async function produceMessage(message: string){
    const producer = await createProducer();
    await producer.send({
        messages: [{key: `message-${Date.now()}`, value: message}],
        topic: "MESSAGES"
    })
    return true
}


export async function startMessageConsumer() {
    const consumer = kafka.consumer({groupId: 'default'});
    await consumer.connect();
    await consumer.subscribe({topic: 'MESSAGES', fromBeginning: true})

    await consumer.run({
        autoCommit: true,
        eachMessage: async ({message, pause}) => {
            console.log("New Message Received")
            if(!message.value) return
            try {
                await prismaClient.message.create({
                    data: {
                        text: message.value?.toString()
                    }
                })
            } catch (error) {
                console.log("Something went wrong")
                pause()
                setTimeout(() =>{
                    consumer.resume([{'topic': 'MESSAGES'}])
                }, 60 * 1000)
            }
        }
    })
}

export default kafka