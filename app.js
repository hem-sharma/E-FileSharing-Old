var express = require('express'),
    app = express(),
    fs = require('fs'),
    config = require(__dirname + '/config.json'),
    port = config.appPort,
    Client = require('scp2').Client,
    sql = require('mssql'),
    toBeProcessed = 0;

console.log('Uploader running @ ' + port, new Date());

app.listen(port, function () {
    processInitiator();
    setInterval(function () {
        processInitiator();
    }, config.intervalTime)
});

function processInitiator() {
    try {
        sql.connect("mssql://" + config.sqlUserName + ":" + config.sqlPassword + "@" + config.sqlServerHost + "/" + config.databaseName).then(function () {
            new sql.Request().query(config.sqlGetCmd).then(function (recordset) {
                if (toBeProcessed === 0) {
                    console.log('found ' + recordset.length + ' records', new Date())
                    toBeProcessed = recordset.length;
                    for (var item in recordset) {
                        var data = recordset[item];
                        //local video path, server filename after upload
                        sendFile(data.ContentLocalLocation, data.ContentBlobName, data);
                    }
                } else {
                    processInitiator();
                }
            }).catch(function (err) {
                console.log(err);
                processInitiator();
            });
        });
    } catch (e) {
        console.log(e);
        processInitiator()
    }
}

function sendFile(loc, fileName, record) {
    var contents = fs.readFileSync(__dirname + '/config.json');
    config = JSON.parse(contents);
    var destinationPath = config.destinationPath + fileName;
    var client = new Client({
        port: 22,
        host: config.destinationHost,
        username: config.destinationUserName,
        password: config.destinationPassword,
    });

    client.upload(loc, destinationPath, function (res) {
        console.log('file uploaded successfully from: ' + loc + ' to path: ' + destinationPath, new Date());
        toBeProcessed = toBeProcessed - 1;
        deleteFile(loc);
        updateStatus(record);
    });
}

function deleteFile(loc) {
    fs.unlinkSync(loc);
    console.log('successfully deleted ' + loc);
}

function updateStatus(record) {
    var cmd = config.sqlStatusUpdateCmd.replace('@contenturl', record.ContentBlobName);
    cmd = cmd.replace('@id', record.ID);
    sql.connect("mssql://" + config.sqlUserName + ":" + config.sqlPassword + "@" + config.sqlServerHost + "/" + config.databaseName).then(function () {
        new sql.Request().query(cmd).then(function (recordset) {
            console.log('record updated for post ' + record.ID, new Date())
        });
    });
}