var fs = require('fs');
const pdf = require('html-pdf');
var config = require('./config.json');
var options = { format: 'Letter', timeout: '100000000' };
var Query = config.DataBaseConfig.Querydb;
var sql = require('mssql/msnodesqlv8');
var async = require('async');
var dbConfig = {
    driver: "msnodesqlv8",
    server: "VCUHSHMOVLDBTST",
    database: "VITAL_test",
    userName: "YOURUSERNAME",
    password: "YOURPASSWORD",
    options: {
        trustedConnection: true,
        useUTC: true
    }

}

// 1. Password Encryption
// used for formatting date used by logger
Date.prototype.YYYYMMDDHHMMSS = function () {
    var yyyy = this.getFullYear().toString();
    var MM = pad(this.getMonth() + 1, 2);
    var dd = pad(this.getDate(), 2);
    var hh = pad(this.getHours(), 2);
    var mm = pad(this.getMinutes(), 2)
    var ss = pad(this.getSeconds(), 2)

    return yyyy + MM + dd + hh + mm + ss;
};
const { createLogger, format, transports, winston } = require('winston');
const { combine, timestamp, label, printf } = format;
const customFormat = printf(({ level, message, label, timestamp }) => {
    return `${timestamp} [${label}] ${level}: ${message}`;
});
const logger = createLogger({
    exitOnError: false,
    format: combine(
        label({ label: 'pdf-converter' }),
        timestamp(),
        customFormat
    ),
    transports: [
        new transports.Console(),
        new transports.File({ dirname: '../', filename: 'html-to-pdf-converter.' + (new Date()).YYYYMMDDHHMMSS() + '.log' })
    ]
});

function getDate() {
    d = new Date();
    alert(d.YYYYMMDDHHMMSS());
}

function pad(number, length) {

    var str = '' + number;
    while (str.length < length) {
        str = '0' + str;
    }

    return str;

}

// Tune these below variables until you achieve desired performance and memory utilization.
const batchSize = config.DataBaseConfig.BatchConfig.batchSize;
const snoozeTime = config.DataBaseConfig.BatchConfig.snoozeTime;// in ms

// Iterates an array of items and optionally set data in context
const forEachPromise = (items, context, callback) => {
    return items.reduce((promise, item) => {
        return promise.then(() => {
            return callback(item, context);
        });
    }, Promise.resolve());
}

const getDataFromDatabase = (query, event) => {
    return new Promise((resolve, reject) => {
        if (event.totalRecords) {
            event['currentBatch'] = event['currentBatch'] + 1;
            resolve(event.paths.slice(event['currentBatch'] * batchSize, (event['currentBatch'] * batchSize) + batchSize));
        } else {
            // get records from database and store in global event variable
            var con = new sql.ConnectionPool(dbConfig);
            var req = new sql.Request(con);
            con.connect(function (err) {
                if (err) {
                    logger.info("Error occurred: " + JSON.stringify(err));
                    reject();
                }
                req.query(query,
                    function (err, recordsets) {
                        if (err) {
                            logger.info("Error occurred in query: " + JSON.stringify(err));
                            reject();
                        }
                        logger.info('connected to database to query for: ' + query);
                        con.close();

                        event.paths = recordsets['recordset'];
                        logger.info('First iteration: ' + event.paths.length);
                        event['currentBatch'] = 0;
                        event['totalRecords'] = event.paths.length;
                        event['totalBatches'] = Math.ceil(event.paths.length / batchSize);
                        resolve(event.paths.slice(0, batchSize));
                    });
            });
        }
    });
}

const updateRecordInDatabase = (UpdateQuery) => {
    return new Promise((resolve, reject) => {
        var con = new sql.ConnectionPool(dbConfig);
        var req = new sql.Request(con);
        con.connect(function (err) {
            if (err) {
                logger.info("Error occurred in connecting to db: " + JSON.stringify(err));
                reject();
            }
            req.query(UpdateQuery,
                function (err, recordsets) {
                    if (err) {
                        logger.info("Error occurred in update query: " + JSON.stringify(err));
                        reject();
                    }
                    logger.info('Executed update query: ' + UpdateQuery);
                    con.close();
                    resolve();
                });
        });
    });
}

const createPDFFromHtml = (record, context) => {
    return new Promise((resolve) => {
        var status;
        // Create PDF using html-pdf library here
        logger.info('creating pdf here for record: ' + record['File_Name'] + ' at ' + record['Doc_Path']);
        // use the above values in record and create pDf from html.
        try {
            if (fs.statSync(record['Doc_Path'])) {
                var content = fs.readFileSync(record['Doc_Path'], "utf8"); 
                status = "Success";
                pdf.create(content, options).toFile(context['pdfDestination'] + record['File_Name'], function (err, res) {
                    if (err) {
                        status ='Not Converted';
                        logger.info("Error occurred while creating PDF: " + err.code + record['Member_Id']);
                        return;
                    }
                    logger.info("Successfully created PDF: " + record['File_Name']);
                });
            } else {
                status = 'FileNotFound';
                logger.info('File does not exist' + record['Doc_Path']);
            }
        } catch (err) {
            status = 'FileNotFound';
            logger.info('File does not exist' + record['Doc_Path']);
        }

        if (context['selectQuery'].indexOf('2016') >= 1) {
            updateQuery = "UPDATE [VITAL_test].[dbo].[CIS_IQ_Review_2016_Final] SET [Processed] = '" + status + "' WHERE Doc_path = '" + record['Doc_Path']+"'";
        }
        if (context['selectQuery'].indexOf('2017') >= 1) {
            updateQuery = "UPDATE [VITAL_test].[dbo].[CIS_IQ_Review_2017_Final] SET [Processed] = '" + status + "' WHERE Doc_path = '" + record['Doc_Path']+"'";
        }
        if (context['selectQuery'].indexOf('2018') >= 1) {
            updateQuery = "UPDATE [VITAL_test].[dbo].[CIS_IQ_Review_2018_Final] SET [Processed] = '" + status + "' WHERE Doc_path = '" + record['Doc_Path']+"'";
        }
        // Make a call to database to update the record status. record['Id']
        updateRecordInDatabase(updateQuery).then(() => {
            resolve();
        })
    });
}

// Get records from the database table
const process = (selectQuery, event) => {
    if (selectQuery.indexOf('2016') >= 1) {
        pdfDestination = config.DataBaseConfig.Destination + config.DataBaseConfig.Folders.year16 + "/";
    }
    if (selectQuery.indexOf('2017') >= 1) {
        pdfDestination = config.DataBaseConfig.Destination + config.DataBaseConfig.Folders.year17 + "/";
    }
    if (selectQuery.indexOf('2018') >= 1) {
        pdfDestination = config.DataBaseConfig.Destination + config.DataBaseConfig.Folders.year18 + "/";
    }
    event['selectQuery'] = selectQuery;
    event['pdfDestination'] = pdfDestination;
    getDataFromDatabase(selectQuery, event)
        .then((items) => forEachPromise(/* array object */items, /* context object */ event, /* callback */ createPDFFromHtml))
        .then(() => {
            logger.info('Loop Completed & returning: ' + event['currentBatch'] + ' and ' + event['totalBatches']);
            if (event['currentBatch'] < event['totalBatches']) {
                logger.info(event['currentBatch'] + ' - Loop Completed & returning.');
                setTimeout(function () {
                    process(selectQuery, event);
                }, snoozeTime);
            } else {
                logger.info('All batches completed.');
            }
        }).catch(err => {
            logger.info('Error occurred: ', err);
        });
}

//Process the remaining queries (2017, 2018, 2019 etc)
// config.DataBaseConfig.Queries.forEach((query) => {
//     logger.info("Processing started for Query: " + query);
//     process(query, {'paths': []});
//     logger.info("Processing completed for Query: " + query);
// });

async.waterfall([
    function process2016(done) {
        logger.info('Started processing 2016 records.');
        process(Query, {'paths': []});
        done(null, 'Completed processing 2016 records.');
    },
    function process2017(step1Result, done) {
        logger.info(step1Result);
        logger.info('Started processing 2017 records.');
        process(config.DataBaseConfig.Queries[0], {'paths': []});
        done(null, 'Completed processing 2017 records.');
    },
    function process2018(step2Result, done) {
        logger.info(step2Result);
        logger.info('Started processing 2018 records.');
        process(config.DataBaseConfig.Queries[1], {'paths': []});
        done(null, 'Completed processing 2018 records.');
    },
    function completedProcessing(step3Result, done){
        logger.info(step3Result);
        logger.info('Finished processing all records for 2016, 2017 and 2018.');
        done(null);
    }
],
function (err) {
    logger.info('Error occurred: ' + JSON.stringify(err));
});

