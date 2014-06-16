var csv = require('csv');
var fs = require('fs');
var iconv = require('iconv-lite');
var moment = require('moment');
var _ = require('lodash');
var mongodb = require('mongodb');

var client = mongodb.MongoClient;
var connectionString = 'mongodb://localhost/onema-hackathon';

var mapping = {
    bssCode: { key: 'Code national BSS', type: String },
    date: { key: 'Date prélèvement', type: Date },
    prelevementId: { key: 'Numéro de prélèvement', type: String },
    analysisId: { key: 'Numéro d\'analyse', type: String },
    param: { key: 'Paramètre', type: String },
    analysisResult: { key: 'Résultat de l\'analyse', type: Number, float: true },
    analysisStatus: { key: 'Code remarque analyse', type: Number },
    bssTown: { key: 'Commune dossier BSS', type: String },
    bssInseeCode: { key: 'Code commune dossier BSS', type: String },
};

client.connect(connectionString, function(err, db) {
    if (err) console.log(err);

    var collection = db.collection('prelevements');

    var input = fs.createReadStream(__dirname + '/data/analyses.txt')
        .pipe(iconv.decodeStream('iso-8859-15'))
        .pipe(iconv.encodeStream('utf8'));

    csv()
        .from.stream(input, {
            delimiter: '|',
            columns: true
        })
        .on('record', function(record) {
            var analysis = { properties: {} };
            _.forEach(mapping, function(v, k) {
                if (record[v.key] && record[v.key].length > 0) {
                    if (v.type === String) analysis.properties[k] = record[v.key];
                    else if (v.type === Number && !v.float) analysis.properties[k] = parseInt(record[v.key]);
                    else if (v.type === Number && v.float) analysis.properties[k] = parseFloat(record[v.key]);
                    else if (v.type === Date) analysis.properties[k] = moment(record[v.key], 'DD/MM/YYYY HH:mm:ss').toDate();
                }
            });

            collection.insert(analysis, function(err, prelevement) {
                console.log(prelevement);
            });
        })
        .on('error', function(err) {
            console.log(err);
        })
        .on('end', function(count) {
            console.log('Count: ' + count);
            process.exit();
        });

});
