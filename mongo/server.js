const mongoose = require("mongoose");
const cors = require("cors");
const express = require("express");
const fs = require('fs');
const csv = require('csv-parser');

const app = express();
const port = 3000;

let mongoConnection = "mongodb+srv://admin:Parangaricutirimicuaro01@myapp.j2ikbis.mongodb.net/BasesNoRelacionales";
    let db = mongoose.connection;

db.on('connecting', () => {
        console.log('Conectando...');
        console.log(mongoose.connection.readyState);
});
    
db.on('connected', () => {
        console.log('¡Conectado exitosamente!');
        console.log(mongoose.connection.readyState);
});


mongoose.connect(mongoConnection, {useNewUrlParser: true});
let userSchema = mongoose.Schema({
    origin: {
        type: String,
        required: true
    },
    connection: {
        type: String,
        required: true
    }
});

let User = mongoose.model("flights", userSchema);

app.use(express.json())

app.use(cors({
    methods: ['GET', 'POST', 'DELETE', 'UPDATE', 'PUT', 'PATCH']
}));


function ponerData(bandera){
    if(!bandera)
        return;  
    
    const filePath = 'flight_passengers.csv'; // Reemplaza con la ruta de tu archivo CSV
    const results = [];
        
    fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (data) => results.push({origin: data.origin, connection: data.connection}))
        .on('end', () => { console.log('Datos leídos del archivo CSV:');  empezamos(results)});

}

function empezamos(results){
    for(let i of results){
        let user = User(i);
        user.save().then((doc) => console.log(("Usuario creado")));
    }
}

findData();
function findData(){
    User.aggregate([
        {$match: { connection: "True" }},
        {$group: { _id: '$origin', total: {$sum: 1}}},
        {$sort: { total: -1}}
    ]).then((docs => {
        console.log(docs);
    })).catch((err) => console.log(err))
    
}


app.get("/", (req, res) => {
    User.aggregate([
        {$match: { connection: "True" }},
        {$group: { _id: '$origin', total: {$sum: 1}}},
        {$sort: { total: -1}}
    ]).then((docs => {
        res.send(docs);
    })).catch((err) => console.log(err))
})


app.listen(port, () => {
    console.log("Lito " + port);
});




