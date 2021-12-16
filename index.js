const csv=require('csvtojson')
const path = require('path');
const fs = require('fs');

var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017/";

const security_names = {};

const mapData = (dataArray)=> {
    return new Promise(res=>{
        let formatData =[];
        dataArray.forEach(obj =>{
            formatData.push({
                Date : new Date(obj.Date),
                Open : parseFloat(obj.Open),
                High: parseFloat(obj.High),
                Low: parseFloat(obj.Low),
                Close: parseFloat(obj.Close),
                Adj_Close: parseFloat(obj["Adj Close"]),
                Volume: parseInt(obj.Volume)
            });
        });
        res(formatData);
    })
}

const readFromCsv = (fileName) =>{
    return new Promise(res => {
        csv().fromFile(path.join("../files/stocks", fileName)).then(async dataArray =>{
            let formatData = await mapData(dataArray);
            res(formatData);
        })
    });
}

const addToDb = async(namesObj, start, end) =>{
    fs.readdir("../files/stocks", (err, files)=>{
        if(!err){
            MongoClient.connect(url, function(err, db){
                var dbo = db.db("eduFund");
                files.forEach(async (file, index) => {
                    if(index >= start && index < end){
                        try{
                            const data =await readFromCsv(file);
                            const symbol = file.split(".")[0];
                            dbo.collection("stocks").insertOne({
                                name : namesObj[symbol],
                                symbol: symbol,
                                data: data
                            }, function(err, res){
                                if (err) throw err;
                            });
                        }
                        catch(error){
                            console.log(error);
                        }
                    }
                });
            });
        }
    });
}

const getSymbolicNames = async() =>{
    const array = await csv().fromFile('../symbols_valid_meta.csv');
    array.forEach(obj =>{
        security_names[obj.Symbol] = obj['Security Name'];
    });
    await addToDb(security_names, 3000 ,4000);
}

getSymbolicNames();
setTimeout(() => {
    addToDb(security_names, 4000 ,5000);
}, 20000);
setTimeout(() => {
    addToDb(security_names, 5000 ,6000);
}, 40000);
