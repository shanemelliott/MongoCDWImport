const { MongoClient } = require('mongodb');
const moment = require('moment');
const fastcsv = require('fast-csv');
const fs = require('fs');
const config = require('./config');
const db = require('./db');

const RespTable = ''; //Import Table    
const StatsTable = ''; //Import Table


// Create a new MongoClient
const client = new MongoClient(db.url,);
console.log('Connecting to the database...');

//Function to convert FMDateTime to UTC
function fm2UTC(dateTime) {
    if (dateTime) {
        var time = dateTime.toString().split('.')[1]
        if (time) {
            time = time.padEnd(6, '0')
            dateTime = dateTime + 17000000
            dateTime = dateTime.toString().split('.')[0] + "." + time
            dateTime = moment(dateTime, 'YYYYMMDD.HHmmss').format('YYYY-MM-DD')
        } else {
            dateTime = dateTime + 17000000
            dateTime = moment(dateTime, 'YYYYMMDD.HHmmss').format('YYYY-MM-DD')
        }
        return dateTime
    } else {
        return ''
    }
}


async function queryDatabase(date) {
    try {


        const db = client.db(dbName);
        const collection = db.collection('patientAppointments');
        const tempCollection = db.collection('tempPatientAppointments');

        // Log the start time
        const startTime = moment().utcOffset('-08:00').format('YYYY-MM-DD HH:mm:ss');
        console.log(`Query started at: ${startTime}`);

        // Start timing the query
        console.time('Query Time');

        await collection.aggregate([
            {
                $match: {
                    date: date
                }
            },
            /*{
                 $limit: 100000 // Limit to 100,000 records
             },*/
            {
                $project: {
                    date: 1,
                    "appointment.stopCode": 1,
                    cellPhone: 1,
                    fmAutoReplyDate: 1,
                    fmDateSent: 1,
                }
            },
            {
                $out: "tempPatientAppointments"
            }
        ]).toArray();
        console.timeEnd('Query Time');

        console.time('Query Time2');

        // Create indexes for the temp collection
        await tempCollection.createIndex({ date: 1 });
        await tempCollection.createIndex({ "appointment.stopCode": 1 });
        await tempCollection.createIndex({ fmAutoReplyDate: 1 });


        // Query Database for response
        const records = await tempCollection.aggregate([
            {
                $match: {
                    fmAutoReplyDate: { $ne: 0 }
                }
            },
            {
                $group: {
                    _id: {
                        date: "$date",
                        stopCode: "$appointment.stopCode"
                    },
                    uniqueCellPhones: { $addToSet: "$cellPhone" },
                    avgResponseTime: {
                        $avg: {
                            $subtract: ["$fmAutoReplyDate", "$fmDateSent"]
                        }
                    }
                }
            },
            {
                $project: {
                    _id: 0,
                    date: "$_id.date",
                    stopCode: "$_id.stopCode",
                    count: { $size: "$uniqueCellPhones" },
                    avgResponseTime: 1
                }
            }
        ]).toArray();

        //convert the records to UTC and round the avgResponseTime to 3 decimal places
        const convertedRecords = await records.map(record => {
            record.date = fm2UTC(record.date);
            record.avgResponseTime = parseFloat(record.avgResponseTime.toFixed(3));
            return record;
        })
        console.log(convertedRecords.length);
        const ws1 = fs.createWriteStream(RespTable+".csv");
        fastcsv
        .write(convertedRecords, { headers: true })
        .pipe(ws1);


        //Second Search to get the unique cell phones - this is used to get unique patients: 
        console.log('Starting Second Search');

        const records2 = await tempCollection.aggregate([
            {
                $group: {
                    _id: {
                        date: "$date",
                        stopCode: "$appointment.stopCode"
                    },
                    uniqueCellPhones: { $addToSet: "$cellPhone" }
                }
            },
            {
                $project: {
                    _id: 0,
                    date: "$_id.date",
                    stopCode: "$_id.stopCode",
                    count: { $size: "$uniqueCellPhones" }
                }
            }
        ]).toArray();

        //End timing the query
        console.timeEnd('Query Time2');

        const convertedRecords2 = await records2.map(record => {
            record.date = fm2UTC(record.date);
            return record;
        })

        const ws2 = fs.createWriteStream(RespTable+".csv");
        fastcsv
        .write(convertedRecords2, { headers: true })
        .pipe(ws2);
        
        //drop the temp collection
        console.log('dropping')
        await tempCollection.drop();
    } catch (error) {
        console.error(error);

    }
}
async function processDatesForYear(year) {
    try {
        // Connect to the MongoDB server
        await client.connect();
        console.log('Connected successfully to server');
        let currentDate = moment(fm2UTC(Number(year + '0101')));
        const endDate = moment(fm2UTC(Number(year + '1231')));
        while (currentDate.isSameOrBefore(endDate)) {
            //convert UTC to FMDateTime
            const fileManDate = parseInt('3' + moment(currentDate).format('YY') + currentDate.format("MMDD"));
            console.log('Using Date: ' + fileManDate);
            await queryDatabase(fileManDate);
            currentDate.add(1, 'day');
        }
    } catch (error) {
        console.error(error);
    } finally {
        // Close the connection
        console.log('disconnecting');
        await client.close();
        console.log('Disconnected from server');
    }
}
//the begining of the FM Date indicating the year. 
const year = 323;
processDatesForYear(year);