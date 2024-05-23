import { Server } from "socket.io";
import Redis from 'ioredis'
import {produceMessage} from './kafka'


const pub = new Redis({
    host: 'redis-1467c5b6-sarimxahid123-7a0a.c.aivencloud.com',
    port: 25643,
    username: 'default',
    password: 'AVNS_7JQ32cxZ8_Y88_ZtTeI',
});
const sub = new Redis({
    host: 'redis-1467c5b6-sarimxahid123-7a0a.c.aivencloud.com',
    port: 25643,
    username: 'default',
    password: 'AVNS_7JQ32cxZ8_Y88_ZtTeI',
});


class SocketService {

    private _io: Server;

    constructor() {
        console.log("Init Socket Server")
        this._io = new Server({
            cors: {
                allowedHeaders: ['*'],
                origin: "*"
            }
        });
        sub.subscribe('MESSAGES')

    }

    public initListeners(){
        const io = this.io;
        console.log("initListeners........")
        io.on('connect', (socket) => {
            console.log("New Socket connected", socket.id);

            socket.on('event:message', async ({message} : {message: string}) => {
                console.log('New Message Received', message)
                // Publis this message to redis
                await pub.publish('MESSAGES', JSON.stringify({message}))
            })

        })
        sub.on('message', async (channle, message) => {
            if(channle === 'MESSAGES'){
                io.emit('message', {message} )
               await produceMessage(message);
               console.log("Message produced to Kafka Broker");
            }
        })
    }   

    get io() {
        return this._io;
    }

}

export default SocketService