// jshint esversion: 6
const express = require("express");
const _ = require('lodash');
const {
     Act
    ,Obj
    ,Pro
} = require('./Staticdata')

const app = express();

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

const gHome = (req, res) => {
    var randomJson = []
    while(randomJson.length < 50) {
        data = CreateRandomJson()
        randomJson.push(data)
    }
    res.json(randomJson);
  }


app.get('/', gHome);

app.listen(process.env.PORT || 3000, function() {
  });