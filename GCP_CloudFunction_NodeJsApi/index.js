// jshint esversion: 6
'use strict';
// const express = require("express");
const _ = require('lodash');
const {Act,Obj,Pro} = require('./Staticdata'); // Import Static data
const {PubSub} = require('@google-cloud/pubsub'); // [START functions_pubsub_publish]

//const app = express();
const pubsub = new PubSub();  // Instantiates a client

var current_time = new Date();
var hourago = new Date(current_time.getTime() - (1000*60*60));

const randomDate = (start, end) => {
    return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()));
  }


const CreateRandomJson = () => {
    var randomJ = {
        "activity" : _.sample(Act),
        "object" : _.sample(Obj),
        "profile" : _.sample(Pro),
        "time" : randomDate(hourago,current_time)
    }
    return randomJ
};

//const gHome = (req, res) => {
    //var randomJson = []
   // while(randomJson.length < 1000) {
      //  data = CreateRandomJson()
      //  randomJson.push(data)
  //  }
 //   res.json(randomJson);
 // }

exports.RandomJson = async (req, res) => {
     
    const topic = pubsub.topic(<PubSub Topic Name>);
     
    var randomJson = []
    
    while(randomJson.length < 1000) {
        data = CreateRandomJson()
        randomJson.push(data)
    }
    // res.json(randomJson);
    const messageObject = {
      data: {
          message: randomJson,
      },
    };
    
    const messageBuffer = Buffer.from(JSON.stringify(messageObject), 'utf8');
    
       // Publishes a message
    try {
         await topic.publish(messageBuffer);
         res.status(200).send('Message published.');
    } catch (err) {
         console.error(err);
         res.status(500).send(err);
         return Promise.reject(err);
    }
     
};


// app.get('/', gHome);

// app.listen(process.env.PORT || 3000, function() {
 // });
