const { MongoClient } = require('mongodb');
const moment = require('moment-timezone'); 
const config = require('./config');
const db = require('./db');
//const { connectToDB, sql } = require('./db');
//const stations = require('./stations');


// Connection URL
const RespTable = 'App.MON_ApptRespStatsNew';
const StatsTable = 'App.MON_ApptStatsNew';
const dbName = config.mongo.dbName;
// Create a new MongoClient
const client = new MongoClient(db.url,);
console.log('Connecting to the database...');


function fm2UTC(dateTime,timeZone) {
    if (dateTime) {
        var time = dateTime.toString().split('.')[1]
        if (time) {
            time = time.padEnd(6, '0')
            dateTime = dateTime + 17000000
            dateTime = dateTime.toString().split('.')[0] + "." + time
            dateTime = moment(dateTime, 'YYYYMMDD.HHmmss').tz(timeZone).format()
        } else {
            dateTime = dateTime + 17000000
            dateTime = moment(dateTime, 'YYYYMMDD.HHmmss').format('YYYY-MM-DD')
        }
        return dateTime
    } else {
        return ''
    }
}
async function dropLastNinetyCollection() {
    try {
        const db = client.db(dbName);   
    const lastNinetyCollection = db.collection('PatientAppointmentsLastNinety');
    const collections = await db.listCollections({ name: 'PatientAppointmentsLastNinety' }).toArray();
    if (collections.length > 0) {
        await lastNinetyCollection.drop();
        console.log('Dropped existing PatientAppointmentsLastNinety collection.');
    }
    } catch (error) {
        console.error('Error dropping PatientAppointmentsLastNinety collection:', error);

    }
}

async function createLastNinetyndex() {
    try {

        const db = client.db(dbName);
        const lastNinetyCollection = db.collection('PatientAppointmentsLastNinety');
        const indexes = await lastNinetyCollection.listIndexes().toArray();
        const hasDateIndex = indexes.some(idx =>
            JSON.stringify(idx.key) === JSON.stringify({ date: 1 })
        );
        if (!hasDateIndex) {
            await lastNinetyCollection.createIndex({ date: 1 });
            console.log('Index on date created for PatientAppointmentsLastNinety collection.');
        }
        else {
            console.log('Index on date already exists for PatientAppointmentsLastNinety collection.');
        }   
    } catch (error) {
        console.error('Error creating index on date:', error);
    }
}


async function queryDatabase(date) {
    try {
        const db = client.db(dbName);
        const collection = db.collection('patientAppointments');
        const startTime = moment().utcOffset('-08:00').format('YYYY-MM-DD HH:mm:ss');
        console.log(`Query started at: ${startTime}`);

        // Start timing the query
        console.time('Query Time');


        const tempDocs = await collection.aggregate([
            {
                $match: {
                    date: date
                }
            },
            /*{
                 $limit: 10
             },*/
           
            {
                $project: {
                    date: 1,
                    stopCode: "$appointment.stopCode",
                    stationNo:1,
                    patientIcn:1,
                    patientName:1,
                    textMessage:1,
                    confirmCode:1,
                    smsResponse:1,
                    cancelCode:1,
                    cellPhone:1,
                    status:1,
                    errorCode:1,
                    grouped:1,
                    location:"$appointment.location",
                    divisionNo:"$appointment.divisionNo",
                    physicalLocation:"$appointment.physicalLocation",
                    facility:"$appointment.facility",
                    fmAutoReplyDate:1,
                    date:1,
                    fmDateSent:1,
                    dateCreated:1,
                    dateModified:1,
                    dateSent:1,
                    fmAutoReplyDateString1:1
                    
                }
            }/*,
            {
                $out: "tempPatientAppointments"
            }*/
        ]).toArray();
        console.timeEnd('Query Time');

    
      console.time('Query Time2');
        // Get all documents from tempPatientAppointments
        //const tempDocs = await tempCollection.find({}).toArray();
        console.log(`Found ${tempDocs.length} records in tempPatientAppointments`);
        console.timeEnd('Query Time2');
        if (tempDocs.length > 0) {
            const lastNinetyCollection = db.collection('PatientAppointmentsLastNinety');
            // Insert all documents into PatientAppointmentsLastNinety
            await lastNinetyCollection.insertMany(tempDocs);
            console.log(`Appended ${tempDocs.length} records to PatientAppointmentsLastNinety`);
        } else {
            console.log('No records found in tempPatientAppointments to append.');
        }

        return tempDocs;

    } catch (error) {
        console.error(error);
    
    }
}

async function processDatesForYear(year) {
    try {
    // Connect to the MongoDB server
    await client.connect();
    console.log('Connected successfully to server');
  

    let currentDate =moment(fm2UTC(Number(year+'0318')));
    const endDate = moment(fm2UTC(Number(year+'0527')));
    while (currentDate.isSameOrBefore(endDate)) {

        const fileManDate = parseInt('3'+moment(currentDate).format('YY')+currentDate.format("MMDD"));

        console.log('Using Date: ' + fileManDate);
        //await dropLastNinetyCollection();
        //await createLastNinetyndex();
        const records = await queryDatabase(fileManDate);
        console.log("records: "+records.length);
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

/*
const startDate = 3241201;
const endDate = 3241205; // Adjust the end date as needed
processDates(startDate, endDate);
*/

const year = 325; // Adjust the year as needed
processDatesForYear(year);