// jshint esversion: 6
'use strict';
const _ = require('lodash');
const {Act,Obj,Pro} = require('./Staticdata'); // Import Static data
const {PubSub} = require('@google-cloud/pubsub'); // [START functions_pubsub_publish]
const moment = require('moment');


const pubsub = new PubSub();  // Instantiates a client

const CreateRandomJson = () => {
    var randomJ = {
        "activity" : _.sample(Act),
        "object" : _.sample(Obj),
        "profile" : _.sample(Pro),
        "time" : moment().format('Y-M-D H:m:s')
    }
    return randomJ
};

const topic = pubsub.topic(<TOPIC NAME>);

const publishToPubSub = () => {
    var d = CreateRandomJson()

    const messageBuffer = Buffer.from(JSON.stringify(d), 'utf8');
      
    //Publishes a message
    try {
        topic.publish(messageBuffer);
        console.log('Message published.');
        console.log(d)
    } catch (err) {
        console.log(err);
      }
     setTimeout(publishToPubSub, 5000);
}

publishToPubSub()


