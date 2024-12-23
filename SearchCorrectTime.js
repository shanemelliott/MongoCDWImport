const { MongoClient } = require('mongodb');
const moment = require('moment-timezone'); 
const config = require('./config');
const db = require('./db');
const { connectToDB, sql } = require('./db');
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


        const testcollection = await collection.aggregate([
            {
                $match: {
                    date: date
                }
            },
            /*{
                 $limit: 10
             },*/
             {
                $addFields: {
                    fmAutoReplyDateString1: {
                        $cond: {
                            if: { $eq: ["$fmAutoReplyDate", 0] },
                            then: 0,
                            else:
                            {
                              
                                   $substr: [{ $toString: { $toDecimal: "$fmAutoReplyDate" } }, 8, 2] 
                                   
                            
                            }
                        }
                    }
                }
            },
            {
                $project: {
                    _id: 1,
                    date: 1,
                    stopCode: "$appointment.stopCode",
                    stationNo:1,
                    cellPhone:1,
                    fmAutoReplyDate:1,
                    date:1,
                    fmDateSent:1,
                    dateCreated:1,
                    dateModified:1,
                    dateSent:1,
                    fmAutoReplyDateString1:1
                    
                }
            },
            {
                $out: "tempPatientAppointments"
            }
        ]).toArray();
        console.timeEnd('Query Time');

        console.time('Query Time2');
        await tempCollection.createIndex({ fmAutoReplyDate: 1 });
        await tempCollection.createIndex({ stopCode: 1 });
        await tempCollection.createIndex({ stationNo: 1 });
        await tempCollection.createIndex({ fmAutoReplyDate: 1 });


        // Query Database for response
        const records = await tempCollection.aggregate([
            {
                $match: {
                    fmAutoReplyDate: { $ne: 0 }
                }
            }, {
                $lookup: {
                    from: 'smsStations',
                    localField: 'stationNo',
                    foreignField: 'stationNo',
                    as: 'stationInfo'
                }
            },
            {
                $unwind: '$stationInfo'
            },
            {
                $addFields: {
                    timeZone: '$stationInfo.timezone'
                }
            },

            {
                $addFields: {
                    fmAutoReplyDateString: {
                        $cond: {
                            if: { $eq: ["$fmAutoReplyDate", 0] },
                            then: 0,
                            else:
                            {
                                $concat: [
                                    "20",
                                    { $substr: [{ $toString: { $toDecimal: "$fmAutoReplyDate" } }, 1, 2] },
                                    "-",
                                    { $substr: [{ $toString: { $toDecimal: "$fmAutoReplyDate" } }, 3, 2] },
                                    "-",
                                    { $substr: [{ $toString: { $toDecimal: "$fmAutoReplyDate" } }, 5, 2] },
                                    "T",
                                    {
                                        $cond: {
                                            if: { $eq: [{ $substr: [{ $toString: { $toDecimal: "$fmAutoReplyDate" } }, 8, 2] }, "24"] },
                                            then: "23",
                                            else: { $substr: [{ $toString: { $toDecimal: "$fmAutoReplyDate" } }, 8, 2] }
                                        }
                                    },
                                    ":",
                                    { $substr: [{ $toString: { $toDecimal: "$fmAutoReplyDate" } }, 10, 2] }
                                ]
                            }
                        }
                    }
                }
            },
            {
                $addFields: {
                    fmAutoReplyDateFormatted: {
                        $cond: {
                            if: { $eq: ["$fmAutoReplyDate", 0] },
                            then: 0,
                            else: {
                                $dateFromString: {
                                    dateString: "$fmAutoReplyDateString",
                                    timezone: "$timeZone"
                                }
                            }
                        }
                    }
                }
            },
            {
                $addFields: {
                    responseTime: {
                        $cond: {
                            if: { $gt: ["$fmDateSent", 0] },
                            then: {
                                $divide: [
                                    { $subtract: ["$fmAutoReplyDateFormatted", "$dateSent"] },
                                    1000 * 60 // Convert milliseconds to minutes
                                ]
                            },
                            else: {
                                $divide: [
                                    { $subtract: ["$fmAutoReplyDateFormatted", "$dateCreated"] },
                                    1000 * 60 // Convert milliseconds to minutes
                                ]
                            }
                        }
                    }
                }
            },
            /*
            {
                $project: {
                    _id: 1,
                    date: 1,
                    stopCode: 1,
                    stationNo:1,
                    cellPhone:1,
                    fmAutoReplyDate:1,
                    date:1,
                    fmDateSent:1,
                    dateCreated:1,
                    dateModified:1,
                    dateSent:1,
                    timeZone:1,
                    fmAutoReplyDateString:1,
                    fmAutoReplyDateString1:1
                    //fmAutoReplyDateFormatted:1,
                   // responseTime:1
                    
                }
            }
            */
            {
                $group: {
                    _id: {
                        date: "$date",
                        stopCode: "$stopCode",
                        stationNo: "$stationNo"
                    },
                    uniqueCellPhones: { $addToSet: "$cellPhone" },
                    avgResponseTime: { $avg: "$responseTime" }
                   
                }
            },
            {
                $project: {
                    _id: 0,
                    date: "$_id.date",
                    stopCode: "$_id.stopCode",
                    stationNo: "$_id.stationNo",
                    count: { $size: "$uniqueCellPhones" },
                    avgResponseTime: 1
                }
            }
        ]).toArray();

        console.log(records.length);
        console.timeEnd('Query Time2');
        const convertedRecords = await records.map(record => {
            record.date = fm2UTC(record.date);
            return record;
        })
       
        
        console.log('dropping')
        await tempCollection.drop();
       
     
        return convertedRecords;

    } catch (error) {
        console.error(error);
    
    }
}

async function insertData(data) {
    console.log('Connecting to the database...');
    const pool = await connectToDB();
     // Use bulk insert operation
     const table = new sql.Table(RespTable);
     table.create = false; // Set to false to avoid creating a new table
     table.columns.add('date', sql.NVarChar(50), { nullable: true });
     table.columns.add('stopCode', sql.NVarChar(50), { nullable: true });
     table.columns.add('stationNo', sql.NVarChar(50), { nullable: true });
     table.columns.add('count', sql.Int, { nullable: true });
     table.columns.add('avgResponseTime', sql.Float, { nullable: true });

     data.forEach(record => {
         table.rows.add(record.date, record.stopCode, record.stationNo, record.count, record.avgResponseTime);
     });

     const request = new sql.Request(pool);
     await request.bulk(table);

    
}
async function processDatesForYear(year) {
    try {
    // Connect to the MongoDB server
    await client.connect();
    console.log('Connected successfully to server');
  

    let currentDate =moment(fm2UTC(Number(year+'0101')));
    const endDate = moment(fm2UTC(Number(year+'1231')));
    while (currentDate.isSameOrBefore(endDate)) {

        const fileManDate = parseInt('3'+moment(currentDate).format('YY')+currentDate.format("MMDD"));

        console.log('Using Date: ' + fileManDate);
        const records = await queryDatabase(fileManDate);
        console.log("records: "+records.length);
        await insertData(records);
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

const year = 323; // Adjust the year as needed
processDatesForYear(year);